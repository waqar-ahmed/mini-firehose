from sinks.local.handlers.local_csv_handler import LocalCSVHandler
from sinks.local.handlers.local_json_handler import LocalJsonHandler
from sinks.local.handlers.local_parquet_handler import LocalParquetHandler
from sinks.sink import Sink


class LocalSink(Sink):
    def __init__(self, directory, output_format, partition_cols=None, filename_based_on='datetime'):
        self.directory = directory
        self.handlers = {
            'csv': LocalCSVHandler,
            'parquet': LocalParquetHandler,
            'json': LocalJsonHandler
        }
        if output_format not in self.handlers:
            raise ValueError(f"Unsupported output format: {output_format}")

        handler_class = self.handlers[output_format]
        self.handler = handler_class(directory, partition_cols, filename_based_on)

    def deliver(self, data, filename=None):
        self.handler.write(data, filename)
