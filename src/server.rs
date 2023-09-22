use crate::{
    data_switch::{DataSwitch, Timerange, Timestamp},
    pb::{
        rove_server::{Rove, RoveServer},
        ValidateSeriesRequest, ValidateSeriesResponse, ValidateSpatialRequest,
        ValidateSpatialResponse,
    },
    scheduler::RoveService,
};
use dagmar::Dag;
use futures::Stream;
use std::{net::SocketAddr, pin::Pin};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{transport::Server, Request, Response, Status};

type SeriesResponseStream =
    Pin<Box<dyn Stream<Item = Result<ValidateSeriesResponse, Status>> + Send>>;
type SpatialResponseStream =
    Pin<Box<dyn Stream<Item = Result<ValidateSpatialResponse, Status>> + Send>>;

pub enum ListenerType {
    Addr(SocketAddr),
    UnixListener(UnixListenerStream),
}

#[tonic::async_trait]
impl Rove for RoveService<'static> {
    type ValidateSeriesStream = SeriesResponseStream;
    type ValidateSpatialStream = SpatialResponseStream;

    #[tracing::instrument]
    async fn validate_series(
        &self,
        request: Request<ValidateSeriesRequest>,
    ) -> Result<Response<Self::ValidateSeriesStream>, Status> {
        tracing::debug!("Got a request: {:?}", request);

        let req = request.into_inner();

        let rx = self
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
            .await?;

        let output_stream = ReceiverStream::new(rx);
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

        let rx = self
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
            .await?;

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ValidateSpatialStream
        ))
    }
}

pub async fn start_server(
    listener: ListenerType,
    data_switch: DataSwitch<'static>,
    dag: Dag<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match listener {
        ListenerType::Addr(addr) => {
            let rove_service = RoveService::new(dag, data_switch);

            tracing::info!(message = "Starting server.", %addr);

            Server::builder()
                .trace_fn(|_| tracing::info_span!("helloworld_server"))
                .add_service(RoveServer::new(rove_service))
                .serve(addr)
                .await?;
        }
        ListenerType::UnixListener(stream) => {
            let rove_service = RoveService::new(dag, data_switch);

            Server::builder()
                .add_service(RoveServer::new(rove_service))
                .serve_with_incoming(stream)
                .await?;
        }
    }

    Ok(())
}
