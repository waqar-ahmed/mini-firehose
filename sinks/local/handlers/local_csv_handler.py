from sinks.local.handlers.local_handler import LocalHandler


class LocalCSVHandler(LocalHandler):
    def __init__(self, directory, partition_cols=None, filename_based_on='datetime'):
        super().__init__(directory, 'csv', partition_cols, filename_based_on)

    def _write_data(self, df, file_path):
        df.to_csv(file_path, index=False)


if __name__ == "__main__":
    directory = "test_local_csv_handler_dir"
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

    local_csv_handler = LocalCSVHandler(directory, partition_cols=["Region"])
    local_csv_handler.write(sample_data)
