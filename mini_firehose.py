class MiniFirehose:
    def __init__(self):
        self.sinks = []

    def add_sink(self, sink: Sink) -> None:
        pass

    def remove_sink(self, sink: Sink) -> None:
        pass

    def deliver_data(self, data: Any, output_format: str, sink_config: SinkConfig) -> None:
        pass
