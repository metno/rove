import grpc
import proto.rove_pb2 as rove
import proto.rove_pb2_grpc as rove_grpc
from proto.rove_pb2 import google_dot_protobuf_dot_timestamp__pb2 as ts
from datetime import datetime, timezone

def main():
    channel = grpc.insecure_channel('localhost:1337')
    stub = rove_grpc.RoveStub(channel)

    series_id = "frost:18700/air_temperature"
    start_time = ts.Timestamp(seconds=int(datetime(2023, 6, 26, hour=14, tzinfo=timezone.utc).timestamp()))
    end_time = ts.Timestamp(seconds=int(datetime(2023, 6, 26, hour=16, tzinfo=timezone.utc).timestamp()))
    tests = ["dip_check", "step_check"]
    request = rove.ValidateSeriesRequest(series_id=series_id, start_time=start_time, end_time=end_time, tests=tests)

    print("Sending ValidateSeries request")
    responses = stub.ValidateSeries(request)

    print("Response:\n")
    for response in responses:
        print("Test name: ", response.test, "\n")
        for result in response.results:
            print("    Time: ", datetime.fromtimestamp(result.time.seconds, tz=timezone.utc))
            print("    Flag: ", rove.Flag.Name(result.flag), "\n")

if __name__ == "__main__":
    main()
