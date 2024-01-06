import time

import pytest
import pandas as pd
from pathlib import Path

from mini_firehose.mini_firehose import FirehoseConfig, MiniFirehose
from sinks.local.local_sink import LocalSink


# Your MiniFirehose, LocalSink, and LocalCSVHandler classes go here

@pytest.fixture
def mini_firehose(tmp_path):
    config = FirehoseConfig(buffer_count_limit=2, buffer_time_limit=5, buffer_size_limit_mb=1)
    local_sink = LocalSink(tmp_path, 'csv')
    firehose = MiniFirehose(name="test_firehose", sinks=[local_sink], config=config)
    firehose.start()  # Start the firehose thread
    yield firehose
    firehose.stop()  # Ensure firehose is stopped after the test


def test_firehose_message_count_limit(mini_firehose, tmp_path):
    mini_firehose.add_message({"data": "test1"})
    mini_firehose.add_message({"data": "test2"})
    time.sleep(1)  # Give some time for the file to be written

    files = list(tmp_path.glob('*.csv'))
    assert len(files) == 1  # Check that one file is created

    df = pd.read_csv(files[0])
    assert len(df) == 2  # Check that two messages are written


def test_firehose_time_limit(mini_firehose, tmp_path):
    mini_firehose.add_message({"data": "test1"})
    time.sleep(6)  # Wait for time limit to trigger flush

    files = list(tmp_path.glob('*.csv'))
    assert len(files) == 1

    df = pd.read_csv(files[0])
    assert len(df) == 1

def test_firehose_size_limit(mini_firehose, tmp_path):
    large_message = "x" * (1024 * 1024)  # 1MB message
    mini_firehose.add_message({"data": large_message})
    time.sleep(1)  # Give some time for the file to be written

    files = list(tmp_path.glob('*.csv'))
    assert len(files) == 1

    df = pd.read_csv(files[0])
    assert len(df) == 1


def test_config_all_minus_one(tmp_path):
    with pytest.raises(ValueError) as ex:
        config = FirehoseConfig(buffer_count_limit=-1, buffer_time_limit=-1, buffer_size_limit_mb=-1)


def test_buffer_count_limit(tmp_path):
    with pytest.raises(ValueError) as ex:
        FirehoseConfig(buffer_count_limit=1, buffer_time_limit=-1, buffer_size_limit_mb=-1)
    assert "Buffer count limit should not be less than 10." in str(ex.value)


def test_buffer_time_limit(tmp_path):
    with pytest.raises(ValueError) as ex:
        FirehoseConfig(buffer_count_limit=10, buffer_time_limit=1, buffer_size_limit_mb=-1)
    assert "Buffer time limit should not be less than 60 seconds." in str(ex.value)

def test_buffer_size_mb_limit(tmp_path):
    with pytest.raises(ValueError) as ex:
        FirehoseConfig(buffer_count_limit=10, buffer_time_limit=60, buffer_size_limit_mb=0.5)
    assert "Buffer size limit should not be less than 1 MB." in str(ex.value)