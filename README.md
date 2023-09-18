# Real-time Observation Validation Engine

## What is ROVE?
ROVE is a system for performing real-time quality control (spatial and temporal) on weather data at scale. It was created to meet Met Norway's internal QC needs under the CONFIDENT project, and replace legacy systems. However, it was designed to be modular and generic enough to fit others' needs, and we hope it will see wider use.

## Who is responsible?
[Ingrid Abraham](mailto:ingridra@met.no)

## Status
In alpha testing.

## Benchmarks
Benchmarking code is available [here](https://github.com/metno/rove/blob/trunk/met_binary/benches/scalability_deliverable.rs).

There are three benchmarks: 
- single, which spams ROVE with requests to run dip_check and step_check on a single piece of data each
- series, same as single except with 10k data points per request
- spatial, which spams requests for buddy_check and sct on 10k data points, distributed across a ~350x350km box

Here are the results run on an M1 mac:
```
single_benchmark thrpt:  53.036 Kelem/s
series_benchmark thrpt:  5.4106 Melem/s
spatial_benchmark thrpt:  194.01 Kelem/s
```
Kelem/s = thousand data points per second, M for million.

It is worth noting that ROVE scales horizontally. If you need more throughput than one node can provide, you can set up as many as you need behind a load balancer, though in most cases it's likely your bottleneck will be your data source.

## Test it out
To use ROVE you need to [generate bindings](https://grpc.io/docs/languages/python/quickstart/#generate-grpc-code) for the API in the language you want to use. The API definition can be found [here](https://github.com/metno/rove/blob/trunk/proto/rove.proto).

Once you've set up bindings here's an example of how to use them to make a request to Met Norway's ROVE server in Python:
```python
import grpc
import proto.rove_pb2 as rove
import proto.rove_pb2_grpc as rove_grpc
from proto.rove_pb2 import google_dot_protobuf_dot_timestamp__pb2 as ts
from datetime import datetime, timezone


def send_series(stub):
    request = rove.ValidateSeriesRequest(
        series_id="frost:18700/air_temperature",
        start_time=ts.Timestamp(
            seconds=int(datetime(2023, 6, 26, hour=14, tzinfo=timezone.utc).timestamp())
        ),
        end_time=ts.Timestamp(
            seconds=int(datetime(2023, 6, 26, hour=16, tzinfo=timezone.utc).timestamp())
        ),
        tests=["dip_check", "step_check"],
    )

    print("Sending ValidateSeries request")
    responses = stub.ValidateSeries(request)

    print("Response:\n")
    for response in responses:
        print("Test name: ", response.test, "\n")
        for result in response.results:
            print(
                "    Time: ",
                datetime.fromtimestamp(result.time.seconds, tz=timezone.utc),
            )
            print("    Flag: ", rove.Flag.Name(result.flag), "\n")


def send_spatial(stub):
    request = rove.ValidateSpatialRequest(
        spatial_id="frost:air_temperature",
        time=ts.Timestamp(
            seconds=int(datetime(2023, 6, 26, hour=14, tzinfo=timezone.utc).timestamp())
        ),
        tests=["buddy_check", "sct"],
        polygon=[
            rove.GeoPoint(lat=59.93, lon=10.05),
            rove.GeoPoint(lat=59.93, lon=11.0),
            rove.GeoPoint(lat=60.25, lon=10.77),
        ],
    )

    print("Sending ValidateSpatial request")
    responses = stub.ValidateSpatial(request)

    print("Response:\n")
    for response in responses:
        print("Test name: ", response.test, "\n")
        for result in response.results:
            print(
                "    location: (lat: ",
                result.location.lat,
                " lon: ",
                result.location.lon,
                ")",
            )
            print("    Flag: ", rove.Flag.Name(result.flag), "\n")


def main():
    channel = grpc.insecure_channel("157.249.77.242:1337")
    stub = rove_grpc.RoveStub(channel)

    send_series(stub)
    send_spatial(stub)


if __name__ == "__main__":
    main()
```

## Use it for production
ROVE is not yet production-ready.

## Overview of architecture
![component diagram](https://github.com/metno/rove/blob/trunk/docs/Confident_Component.png?raw=true)
TODO: Link to architecture doc?

## Documentation
TODO: Link to docs.rs once the crate is published

## How to contribute as a developer
ROVE is still in internal development, and as such, we do not maintain a public issue board. Contributions are more than welcome though, contact Ingrid ([ingridra@met.no](mailto:ingridra@met.no)) and we'll work it out.
