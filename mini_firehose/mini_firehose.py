import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import List
from sinks.local.local_sink import LocalSink
from sinks.sink import Sink

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class FirehoseConfig:
    def __init__(self, buffer_count_limit=10, buffer_time_limit=60, buffer_size_limit_mb=1):
        # Validation checks for buffer limits
        if all(limit == -1 for limit in [buffer_count_limit, buffer_time_limit, buffer_size_limit_mb]):
            raise ValueError("All buffer limits cannot be -1 at the same time.")
        if buffer_count_limit != -1 and buffer_count_limit < 10:
            raise ValueError("Buffer count limit should not be less than 10.")
        if buffer_time_limit != -1 and buffer_time_limit < 60:
            raise ValueError("Buffer time limit should not be less than 60 seconds.")
        if buffer_size_limit_mb != -1 and buffer_size_limit_mb < 1:
            raise ValueError("Buffer size limit should not be less than 1 MB.")

        self.buffer_count_limit = buffer_count_limit
        self.buffer_time_limit = buffer_time_limit
        self.buffer_size_limit_mb = buffer_size_limit_mb

class MiniFirehose:
    def __init__(self, name: str, sinks: List[Sink], config: FirehoseConfig = FirehoseConfig()):
        if not sinks:
            raise ValueError("Error! No sinks provided")
        self.name = name
        self.sinks = sinks
        self.config = config
        self.buffer = []
        self.buffer_count = 0
        self.buffer_size_in_mb = 0
        self.last_flush_time = 0
        self.buffer_lock = threading.Lock()
        self.running = False
        self.flushing = False
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.executor = ThreadPoolExecutor(max_workers=min(len(sinks), 10))  # Thread pool size limit

    def add_message(self, message: str):
        with self.buffer_lock:
            self.buffer.append(message)
            self.buffer_count += 1
            self.buffer_size_in_mb += len(str(message)) / (1024 * 1024)
            should_flush = (
                (self.config.buffer_count_limit != -1 and self.buffer_count >= self.config.buffer_count_limit) or
                (self.config.buffer_size_limit_mb != -1 and self.buffer_size_in_mb >= self.config.buffer_size_limit_mb)
            )
            if should_flush:
                self.flush_buffer("buffer-reached")
                self.buffer_count = 0
                self.buffer_size_in_mb = 0
                self.last_flush_time = time.time()

    def flush_buffer(self, event=""):
        #with self.buffer_lock:
        if not self.buffer:
            return
        buffer_to_flush = copy.deepcopy(self.buffer)
        self.buffer.clear()
        self.buffer_count = 0
        self.buffer_size_in_mb = 0

        self.flushing = True
        logger.info(f"Flushing messages: {event}, count: {len(buffer_to_flush)}")
        for sink in self.sinks:
            self.executor.submit(self._flush_buffer_task, buffer_to_flush, sink)
        self.flushing = False

    def _flush_buffer_task(self, buffer_to_flush, sink: Sink):
        try:
            if buffer_to_flush:
                sink.deliver(buffer_to_flush)
        except Exception as e:
            logger.error(f"Failed to flush buffer: {e}")

    def run(self):
        self.running = True
        self.last_flush_time = time.time()
        while self.running:
            time.sleep(1)
            if (time.time() - self.last_flush_time) >= self.config.buffer_time_limit:
                self.flush_buffer("time-limit")
                self.last_flush_time = time.time()

    def start(self):
        if self.config.buffer_time_limit != -1 and not self.running:
            self.thread.start()

    def stop(self):
        if self.running:
            self.running = False
            self.thread.join(timeout=10)
            if self.thread.is_alive():
                logger.warning("Firehose thread did not terminate as expected.")

        self.flush_buffer("final-flush")  # Final flush before shutting down
        self.executor.shutdown(wait=True)
        logger.info(f"{self.name} MiniFirehose stopped.")


if __name__ == "__main__":
    def my_callback(df):
        return df

    config = FirehoseConfig(buffer_count_limit=10, buffer_time_limit=-1, buffer_size_limit_mb=-1)
    local_sink = LocalSink("my_test_dir", 'csv', transformation_callback=my_callback)
    mini_firehose = MiniFirehose(name="test_firehose", sinks=[local_sink], config=config)
    mini_firehose.start()
    for i in range(1, 21):
        mini_firehose.add_message({"data": "message-" + str(i)})
        time.sleep(0.1)
    mini_firehose.stop()
