# Real-time Observation Validation Engine

## What is ROVE?
ROVE is a system for performing real-time quality control (spatial and temporal) on weather data at scale. It was created to meet Met Norway's internal QC needs under the CONFIDENT project (link?), and replace legacy systems. However, it was designed to be modular and generic enough to fit others' needs, and we hope it will see wider use.

## Who is responsible?
[Ingrid Abraham](mailto:ingridra@met.no)

## Status
In development.

## Test it out
Make sure you have a [Rust toolchain installed](https://www.rust-lang.org/learn/get-started).

Compile ROVE:
```sh
$ cargo build
```

Run the test suite, including integration test:
```sh
$ cargo test
```

If you would like to play with it manually, run the binaries at `target/debug/runner` and `target/debug/coordinator`, and make test requests to coordinator with a GRPC compatible tool of your choice. The API is specified in [the coordinator proto file](https://github.com/metno/rove/blob/trunk/proto/coordinator/coordinator.proto). TODO: mention flags for the binaries when that is implemented

## Use it for production
ROVE is not yet production-ready.

## Overview of architecture
![component diagram](https://github.com/metno/rove/blob/trunk/docs/Confident_Component.png?raw=true)
TODO: Link to architecture doc?

## Documentation
TODO: Link to docs.rs once the crate is published

## How to contribute as a developer
ROVE is still in internal development, and as such, we do not maintain a public issue board. Contributions are more than welcome though, contact Ingrid ([ingridra@met.no](mailto:ingridra@met.no)) and we'll work it out.
