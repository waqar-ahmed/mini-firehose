import time
from datetime import datetime


class Handler:
    def __init__(self, file_type, filename_based_on='datetime'):
        self.file_type = file_type
        self.filename_based_on = filename_based_on

    def _get_filename_based_on(self):
        return self.filename_based_on

    def __generate_filename_based_on_datetime(self):
        current_time = datetime.now()
        return f"{current_time.strftime('%Y%m%d%H%M%S')}.{self.file_type}"

    def __generate_filename_based_on_epoch(self):
        return f"{int(time.time())}.{self.file_type}"

    def _generate_filename(self):
        if self._get_filename_based_on() == "datetime":
            return self.__generate_filename_based_on_datetime()
        else:
            return self.__generate_filename_based_on_epoch()
