import os
from common.handler import Handler


class LocalHandler(Handler):
    def __init__(self, directory, file_type, partition_cols=None, filename_based_on='datetime'):
        super().__init__(file_type, filename_based_on)
        self.directory = directory
        self.partition_cols = partition_cols
        self.has_partitions = partition_cols is not None and len(partition_cols) > 0
        if not os.path.exists(self.directory): os.makedirs(self.directory)

    def _get_directory(self):
        return self.directory

    def get_file_path(self, filename=None):
        if not filename or filename == '':
            filename = self._generate_filename()
        return os.path.join(self.directory, filename)

    def get_partition_path(self, group_name):
        if not isinstance(group_name, tuple):
            group_name = (group_name,)
        folder_parts = [self.directory] + [f"{col}={val}" for col, val in zip(self.partition_cols, group_name)]
        partition_path = os.path.join(*folder_parts)
        if not os.path.exists(partition_path): os.makedirs(partition_path)
        return os.path.join(partition_path, self._generate_filename())

    # It is abstract method
    def _write_data(self, df, file_path):
        raise NotImplementedError("This method should be implemented by subclasses")

    def write(self, df, filename=None):
        if self.has_partitions:
            for group_name, group_data in df.groupby(self.partition_cols):
                group_data = group_data.drop(columns=self.partition_cols)
                partition_file_path = self.get_partition_path(group_name)
                self._write_data(group_data, partition_file_path)
        else:
            file_path = self.get_file_path(filename)
            self._write_data(df, file_path)