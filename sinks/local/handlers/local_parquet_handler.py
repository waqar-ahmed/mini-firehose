from sinks.local.handlers.local_handler import LocalHandler


class LocalParquetHandler(LocalHandler):
    def __init__(self, directory, partition_cols=None, filename_based_on='datetime'):
        super().__init__(directory, 'parquet', partition_cols, filename_based_on)

    def _write_data(self, df, file_path):
        df.to_parquet(file_path, index=False)


if __name__ == "__main__":
    directory = "test_local_parquet_handler_dir"
    sample_data = [
        {'Salesperson': 'Alice', 'Region': 'East', 'SalesAmount': 310},
        {'Salesperson': 'Charlie', 'Region': 'East', 'SalesAmount': 320},
        {'Salesperson': 'Alice', 'Region': 'North', 'SalesAmount': 230},
        {'Salesperson': 'Bob', 'Region': 'North', 'SalesAmount': 180},
        {'Salesperson': 'Bob', 'Region': 'South', 'SalesAmount': 150},
        {'Salesperson': 'Charlie', 'Region': 'South', 'SalesAmount': 290},
        {'Salesperson': 'Alice', 'Region': 'West', 'SalesAmount': 210},
        {'Salesperson': 'Bob', 'Region': 'West', 'SalesAmount': 200}
    ]

    local_parquet_handler = LocalParquetHandler(directory, partition_cols=["Region"])
    local_parquet_handler.write(sample_data)
