# MiniFirehose

## Overview

MiniFirehose (similar to AWS Firehose) is a lightweight data ingestion library designed for efficient buffering and delivery of messages to various storage systems. It's particularly useful for applications that need to aggregate data and periodically flush it to a storage backend. MiniFirehose currently supports local file system and Amazon S3 as sinks. The available sink types are csv, json and parquet.

## Features

- **Buffer Management**: Messages are buffered based on count (min 10), size (min 1 MB), or time limit (min 60 seconds), and then flushed to the storage system.
- **Multiple Sinks Support**: Currently supports Local and S3 sinks.
- **Flexible Configuration**: Easily configure buffer size, count, and flush interval.
- **Data Format**: CSV, JSON and Parquet.

## Installation

To use MiniFirehose, you need to have Python installed on your system. You can then clone this repository and use the library in your Python projects.

## Usage

### Setting Up a Local Sink

To use MiniFirehose with a local sink (storing data in CSV files on your local file system):

```python
from mini_firehose.mini_firehose import MiniFirehose, FirehoseConfig
from sinks.local.local_sink import LocalSink

config = FirehoseConfig(buffer_count_limit=1000, buffer_time_limit=60, buffer_size_limit_mb=5)
local_sink = LocalSink(directory='/path/to/output', output_format='csv', partition_cols=["column1"])

firehose = MiniFirehose(name="local_firehose", sinks=[local_sink], config=config)
```

### Setting Up an S3 Sink
To use MiniFirehose with an Amazon S3 sink:

```python
from mini_firehose.mini_firehose import MiniFirehose, FirehoseConfig
from sinks.s3.s3_sink import S3Sink

config = FirehoseConfig(buffer_count_limit=1000, buffer_time_limit=60, buffer_size_limit_mb=5)
s3_sink = S3Sink(bucket_name='your_bucket_name', output_format='csv', partition_cols=["column1"])

firehose = MiniFirehose(name="s3_firehose", sinks=[s3_sink], config=config)
```

### Adding Messages
To add messages to the MiniFirehose buffer:

```python
firehose.add_message({"column1": "value1", "column2": "value2"})
```
### Starting and Stopping MiniFirehose
To start and stop the MiniFirehose:

```python
firehose.start()  # Start processing and flushing messages
# ... your code to add messages ...
firehose.stop()   # Stop processing and ensure all messages are flushed
```
### Complete Example
Here's a complete example of using MiniFirehose with a local sink:

```python
from mini_firehose.mini_firehose import MiniFirehose, FirehoseConfig
from sinks.local.local_sink import LocalSink
import time

# Configure MiniFirehose
config = FirehoseConfig(buffer_count_limit=10, buffer_time_limit=3, buffer_size_limit_mb=1)
local_sink = LocalSink(directory='output', output_format='csv')
firehose = MiniFirehose(name="test_firehose", sinks=[local_sink], config=config)

# Start MiniFirehose
firehose.start()

# Add messages
for i in range(1, 21):
    firehose.add_message({"data": "message-" + str(i)})
    time.sleep(1)

# Stop MiniFirehose
firehose.stop()
```

In this example, MiniFirehose buffers messages and periodically flushes them to a CSV file in the output directory.

### Access via API

MiniFirehose also offers API endpoints, allowing you to run it separately or on a different server. The api is built using FastAPI. To start the api, use the following command:

```bash
pip install -e .            # Installing as cli from the source code
mini-firehose api start
```
Once the api is up and running, you can access the OpenAPI documentation at http://127.0.0.1:8000/docs.

The api provides the following endpoints:

| Action               | Method | Endpoint                                 |
|----------------------|--------|------------------------------------------|
| Create MiniFirehose  | POST   | `/minifirehoses`                         |
| Get MiniFirehose     | GET    | `/minifirehoses`                         |
| Delete MiniFirehose  | DELETE | `/minifirehoses/{firehose_name}`         |
| Add Message          | POST   | `/minifirehoses/{firehose_name}/message` |
| Get Stats            | GET    | `/minifirehoses/{firehose_name}/stats`   |



These endpoints allow you to manage **minifirehoses** and interact with the MiniFirehose system through HTTP requests.