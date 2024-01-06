import logging
from sinks.s3.handlers.s3_csv_handler import S3CSVHandler
from sinks.s3.handlers.s3_parquet_handler import S3ParquetHandler
from sinks.sink import Sink

logger = logging.getLogger(__name__)

class S3Sink(Sink):
    def __init__(self, bucket, prefix, output_format, partition_cols=None, filename_based_on='datetime', s3_config=None):
        self.bucket = bucket
        self.prefix = prefix
        self.handlers = {
            'csv': S3CSVHandler,
            'parquet': S3ParquetHandler
        }
        if output_format not in self.handlers:
            raise ValueError(f"Unsupported output format: {output_format}")

        handler_class = self.handlers[output_format]

        self.handler = handler_class(bucket, prefix, partition_cols, filename_based_on, s3_config)

    def deliver(self, data, filename=None):
        if data is None or len(data) == 0:
            raise ValueError("Error! No data found")
        self.handler.write(data, filename)
