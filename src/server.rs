use crate::{
    data_switch::{DataSwitch, Timerange, Timestamp},
    pb::{
        rove_server::{Rove, RoveServer},
        ValidateSeriesRequest, ValidateSeriesResponse, ValidateSpatialRequest,
        ValidateSpatialResponse,
    },
    scheduler::{self, Scheduler},
};
use dagmar::Dag;
use futures::Stream;
use std::{net::SocketAddr, pin::Pin};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{transport::Server, Request, Response, Status};

type SeriesResponseStream =
    Pin<Box<dyn Stream<Item = Result<ValidateSeriesResponse, Status>> + Send>>;
type SpatialResponseStream =
    Pin<Box<dyn Stream<Item = Result<ValidateSpatialResponse, Status>> + Send>>;

enum ListenerType {
    Addr(SocketAddr),
    UnixListener(UnixListenerStream),
}

impl From<scheduler::Error> for Status {
    fn from(item: scheduler::Error) -> Self {
        match item {
            scheduler::Error::TestNotInDag(s) => {
                Status::not_found(format!("test name `{}` not found in dag", s))
            }
            scheduler::Error::InvalidArg(s) => {
                Status::invalid_argument(format!("invalid argument: {}", s))
            }
            scheduler::Error::Runner(e) => Status::aborted(format!("failed to run test: {}", e)),
            scheduler::Error::DataSwitch(e) => {
                Status::not_found(format!("data switch failed to find data: {}", e))
            }
        }
    }
}

#[tonic::async_trait]
impl Rove for Scheduler<'static> {
    type ValidateSeriesStream = SeriesResponseStream;
    type ValidateSpatialStream = SpatialResponseStream;

    #[tracing::instrument]
    async fn validate_series(
        &self,
        request: Request<ValidateSeriesRequest>,
    ) -> Result<Response<Self::ValidateSeriesStream>, Status> {
        tracing::debug!("Got a request: {:?}", request);

        let req = request.into_inner();
        let req_len = req.tests.len();

        let mut rx = self
            .validate_series_direct(
                req.series_id,
                req.tests,
                Timerange {
                    start: Timestamp(
                        req.start_time
                            .as_ref()
                            .ok_or(Status::invalid_argument("invalid timestamp for start_time"))?
                            .seconds,
                    ),
                    end: Timestamp(
                        req.end_time
                            .as_ref()
                            .ok_or(Status::invalid_argument("invalid timestamp for start_time"))?
                            .seconds,
                    ),
                },
            )
            .await
            .map_err(Into::<Status>::into)?;

        // TODO: remove this channel chaining once async iterators drop
        let (tx_final, rx_final) = channel(req_len);
        tokio::spawn(async move {
            while let Some(i) = rx.recv().await {
                match tx_final.send(i.map_err(|e| e.into())).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                };
            }
        });

        let output_stream = ReceiverStream::new(rx_final);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ValidateSeriesStream
        ))
    }

    #[tracing::instrument]
    async fn validate_spatial(
        &self,
        request: Request<ValidateSpatialRequest>,
    ) -> Result<Response<Self::ValidateSpatialStream>, Status> {
        tracing::debug!("Got a request: {:?}", request);

        let req = request.into_inner();
        let req_len = req.tests.len();

        let mut rx = self
            .validate_spatial_direct(
                req.spatial_id,
                req.tests,
                req.polygon,
                Timestamp(
                    req.time
                        .as_ref()
                        .ok_or(Status::invalid_argument("invalid timestamp for start_time"))?
                        .seconds,
                ),
            )
            .await
            .map_err(Into::<Status>::into)?;

        // TODO: remove this channel chaining once async iterators drop
        let (tx_final, rx_final) = channel(req_len);
        tokio::spawn(async move {
            while let Some(i) = rx.recv().await {
                match tx_final.send(i.map_err(|e| e.into())).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                };
            }
        });

        let output_stream = ReceiverStream::new(rx_final);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ValidateSpatialStream
        ))
    }
}

async fn start_server_inner(
    listener: ListenerType,
    data_switch: DataSwitch<'static>,
    dag: Dag<&'static str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rove_service = Scheduler::new(dag, data_switch);

    match listener {
        ListenerType::Addr(addr) => {
            tracing::info!(message = "Starting server.", %addr);

            Server::builder()
                .trace_fn(|_| tracing::info_span!("helloworld_server"))
                .add_service(RoveServer::new(rove_service))
                .serve(addr)
                .await?;
        }
        ListenerType::UnixListener(stream) => {
            Server::builder()
                .add_service(RoveServer::new(rove_service))
                .serve_with_incoming(stream)
                .await?;
        }
    }

    Ok(())
}

pub async fn start_server_unix_listener(
    stream: UnixListenerStream,
    data_switch: DataSwitch<'static>,
    dag: Dag<&'static str>,
) -> Result<(), Box<dyn std::error::Error>> {
    start_server_inner(ListenerType::UnixListener(stream), data_switch, dag).await
}

pub async fn start_server(
    addr: SocketAddr,
    data_switch: DataSwitch<'static>,
    dag: Dag<&'static str>,
) -> Result<(), Box<dyn std::error::Error>> {
    start_server_inner(ListenerType::Addr(addr), data_switch, dag).await
}
