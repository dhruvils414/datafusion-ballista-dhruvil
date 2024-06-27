// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Implementation of the Apache Arrow Flight protocol that wraps an executor.

use ::core::future::Future;
use arrow::ipc::reader::StreamReader;
use arrow_flight::PollInfo;
use std::convert::TryFrom;
use std::fs::File;
use std::pin::Pin;
use std::result::Result;

use arrow::ipc::CompressionType;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use ballista_core::error::BallistaError;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::{error::ArrowError, record_batch::RecordBatch};
use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, info};
use std::io::{Read, Seek};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::SendError;
use tokio::{sync::mpsc::Sender, task};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

/// Service implementing the Apache Arrow Flight Protocol
#[derive(Clone)]
pub struct BallistaFlightService {}

impl BallistaFlightService {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for BallistaFlightService {
    fn default() -> Self {
        Self::new()
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for BallistaFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let action =
            decode_protobuf(&ticket.ticket).map_err(|e| from_ballista_err(&e))?;

        match &action {
            BallistaAction::FetchPartition { path, .. } => {
                debug!("FetchPartition reading {}", path);
                let file = File::open(path)
                    .map_err(|e| {
                        BallistaError::General(format!(
                            "Failed to open partition file at {path}: {e:?}"
                        ))
                    })
                    .map_err(|e| from_ballista_err(&e))?;
                let reader =
                    StreamReader::try_new(file, None).map_err(|e| from_arrow_err(&e))?;

                let (tx, rx) = channel(2);
                let schema = reader.schema();
                task::spawn_blocking(move || {
                    if let Err(e) = read_partition(reader, tx) {
                        warn!(error = %e, "error streaming shuffle partition");
                    }
                });

                let write_options: IpcWriteOptions = IpcWriteOptions::default()
                    .try_with_compression(Some(CompressionType::LZ4_FRAME))
                    .map_err(|e| from_arrow_err(&e))?;
                let flight_data_stream = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .with_options(write_options)
                    .build(ReceiverStream::new(rx))
                    .map_err(|err| Status::from_error(Box::new(err)));

                Ok(Response::new(
                    Box::pin(flight_data_stream) as Self::DoGetStream
                ))
            }
        }
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let token = uuid::Uuid::new_v4();
        info!("do_handshake token={}", token);

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<
            Pin<Box<dyn Stream<Item = Result<_, Status>> + Send + 'static>>,
        > = Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();

        while let Some(data) = request.next().await {
            let _data = data?;
        }

        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        let _action = decode_protobuf(&action.body).map_err(|e| from_ballista_err(&e))?;

        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    #[doc = r""]
    #[doc = r" For a given FlightDescriptor, start a query and get information"]
    #[doc = r" to poll its execution status. This is a useful interface if the"]
    #[doc = r" query may be a long-running query. The first PollFlightInfo call"]
    #[doc = r" should return as quickly as possible. (GetFlightInfo doesn't"]
    #[doc = r" return until the query is complete.)"]
    #[doc = r""]
    #[doc = r" A client can consume any available results before"]
    #[doc = r" the query is completed. See PollInfo.info for details."]
    #[doc = r""]
    #[doc = r" A client can poll the updated query status by calling"]
    #[doc = r" PollFlightInfo() with PollInfo.flight_descriptor. A server"]
    #[doc = r" should not respond until the result would be different from last"]
    #[doc = r#" time. That way, the client can "long poll" for updates"#]
    #[doc = r" without constantly making requests. Clients can set a short timeout"]
    #[doc = r" to avoid blocking calls if desired."]
    #[doc = r""]
    #[doc = r" A client can't use PollInfo.flight_descriptor after"]
    #[doc = r" PollInfo.expiration_time passes. A server might not accept the"]
    #[doc = r" retry descriptor anymore and the query may be cancelled."]
    #[doc = r""]
    #[doc = r" A client may use the CancelFlightInfo action with"]
    #[doc = r" PollInfo.info to cancel the running query."]
    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]

    fn poll_flight_info<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<FlightDescriptor>,
    ) -> ::core::pin::Pin<
        Box<
            dyn Future<Output = Result<Response<PollInfo>, Status>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}

fn read_partition<T>(
    reader: StreamReader<std::io::BufReader<T>>,
    tx: Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), FlightError>
where
    T: Read + Seek,
{
    if tx.is_closed() {
        return Err(FlightError::Tonic(Status::internal(
            "Can't send a batch, channel is closed",
        )));
    }

    for batch in reader {
        tx.blocking_send(batch.map_err(|err| err.into()))
            .map_err(|err| {
                if let SendError(Err(err)) = err {
                    err
                } else {
                    FlightError::Tonic(Status::internal(
                        "Can't send a batch, something went wrong",
                    ))
                }
            })?
    }
    Ok(())
}

fn from_arrow_err(e: &ArrowError) -> Status {
    Status::internal(format!("ArrowError: {e:?}"))
}

fn from_ballista_err(e: &ballista_core::error::BallistaError) -> Status {
    Status::internal(format!("Ballista Error: {e:?}"))
}
