import pandas as pd
from sinks.s3.handlers.s3_handler import S3Handler

class S3ParquetHandler(S3Handler):
    def __init__(self, bucket, prefix, partition_cols=None, filename_based_on='datetime', s3_config=None):
        super().__init__(bucket, prefix, 'parquet', partition_cols, filename_based_on, s3_config)

    def _write_data(self, df, file_path):
        df.to_parquet(self.s3_fs.open(file_path, mode="w"), index=False)