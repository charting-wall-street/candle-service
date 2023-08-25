# KIO - Candle IO Service

## Overview

In the context of pattern processing, data is primarily made up of candles. These are crucial in providing necessary information to other services. The K-Line IO Service (KIO) is at the heart of this operation, handling all requests for candle data from intermediate services or directly from the pattern processing pipeline.

KIO, implemented in Go, is designed to efficiently handle multi-threaded workloads, particularly benefiting from Go's prowess in managing such tasks. The service independently handles up to 20 requests at any given moment, each request asking for different candles and time intervals. The performance of KIO is primarily determined by disk I/O and CPU speed, allowing for streaming thousands of candle blocks per second given the small computational load.

## Responsibilities

KIO has several important functions:

- Retrieves and verifies all symbols that require tracking.
- Maintains up-to-date data for these symbols and offers it in multiple time intervals.
- Includes the start date of the first candle block in its response as services require information on data availability.

These responsibilities mean that all necessary metadata can be obtained in a single request.

## Usage

You can run KIO using the command line interface. Here is the usage information:

```shell
$ go run ./cmd/kio --help
Usage of ./cmd/kio:
  -bridge-url string
        path to the api bridge service (default "http://localhost:9701")
  -data-dir string
        path to the local data directory
  -no-audit
        disables audit on startup
  -origins string
        cors origins (default "*")
  -port string
        port from which to run the service (default "9702")
```