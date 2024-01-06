import asyncio
import signal
from typing import Optional, List, Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from uvicorn import Config, Server
from mini_firehose.mini_firehose import MiniFirehose, FirehoseConfig
from sinks.local.local_sink import LocalSink
from sinks.s3.s3_sink import S3Sink


class CreateLocalSinkRequest(BaseModel):
    directory: str
    output_format: str = Field(alias="output-format", example="csv|json|parquet")
    partition_cols: Optional[List[str]] = Field(alias="partition-cols", default=None, example=["Country", "City"])
    filename_based_on: Optional[str] = Field(alias="filename-strategy", default="datetime", example="datetime|epoch")


class S3ConfigRequest(BaseModel):
    access_key: str = Field(alias="access-key")
    secret_key: str = Field(alias="secret-key")
    endpoint_url: Optional[str] = Field(alias="endpoint-url", default=None)


class CreateS3SinkRequest(BaseModel):
    bucket: str
    prefix: Optional[str] = None
    output_format: str = Field(alias="output-format", example="csv|json|parquet")
    partition_cols: Optional[List[str]] = Field(alias="partition-cols", default=None, example=["Country", "City"])
    filename_based_on: Optional[str] = Field(alias="filename-strategy", default="datetime", example="datetime|epoch")
    s3_config: Optional[S3ConfigRequest] = Field(alias="s3-config", default=None)


class CreateMiniFirehoseRequest(BaseModel):
    name: str
    buffer_time: int = Field(alias="buffer-time", default=60)
    buffer_size: int = Field(alias="buffer-size", default=1)
    buffer_count: int = Field(alias="buffer-count", default=10)
    sink_type: str = Field(alias="sink")
    sink_config: Union[CreateLocalSinkRequest, CreateS3SinkRequest] = Field(alias="sink-config")

    @validator('sink_config')
    def validate_sink_config(cls, v, values, **kwargs):
        sink_type = values.get('sink_type')
        if sink_type == 'local' and not isinstance(v, CreateLocalSinkRequest):
            raise ValueError("sink_config must be a CreateLocalSinkRequest for local sink type")
        elif sink_type == 's3' and not isinstance(v, CreateS3SinkRequest):
            raise ValueError("sink_config must be a CreateS3SinkRequest for S3 sink type")
        return v


class MessageModel(BaseModel):
    message: str


class MiniFirehoseApi:
    def __init__(self, host="127.0.0.1", port=8000):
        self.host = host
        self.port = port
        self.app = FastAPI(debug=True)
        self.mini_firehoses = {}
        self._setup_routes()

        config = Config(self.app, host=self.host, port=self.port)
        self.server = Server(config)

        signal.signal(signal.SIGINT, self.signal_handler)  # Ctrl+C signal
        signal.signal(signal.SIGTERM, self.signal_handler)  # Termination signal from the OS

    def signal_handler(self, signum, frame):
        asyncio.ensure_future(self.stop())

    async def start(self):
        try:
            await self.server.serve()
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            print("Server stopped.")

    async def stop(self):
        if self.server:
            self.server.should_exit = True

    def _setup_routes(self):
        sub_domain = "minifirehoses"
        self.app.add_api_route(f"/{sub_domain}", self.create_mini_firehose, methods=['POST'])
        self.app.add_api_route(f"/{sub_domain}", self.get_mini_firehoses, methods=['GET'], response_model=List[str])
        self.app.add_api_route(f"/{sub_domain}" + "/{mini_firehose_name}", self.delete_mini_firehose, methods=['DELETE'])
        self.app.add_api_route(f"/{sub_domain}" + "/{mini_firehose_name}/message", self.add_message, methods=['POST'])
        self.app.add_api_route(f"/{sub_domain}" + "/{mini_firehose_name}/stats", self.get_stats, methods=['GET'])

    async def create_mini_firehose(self, request: CreateMiniFirehoseRequest):
        if request.name in self.mini_firehoses:
            raise HTTPException(status_code=400, detail="MiniFirehose already exists")
        try:
            match request.sink_type:
                case "local":
                    sink = LocalSink(**request.sink_config.model_dump())
                case "s3":
                    sink = S3Sink(**request.sink_config.model_dump())
                case _:
                    raise HTTPException(status_code=400, detail="Unknown sink")

            config = FirehoseConfig(
                buffer_count_limit=request.buffer_count,
                buffer_time_limit=request.buffer_time,
                buffer_size_limit_mb=request.buffer_size
            )

            firehose = MiniFirehose(name=request.name, sinks=[sink], config=config)
            firehose.start()
            self.mini_firehoses[request.name] = firehose
            return {"message": f"MiniFirehose '{request.name}' created"}

        except ValueError as e:
            print(e)
            raise HTTPException(status_code=400, detail=str(e))

    async def get_mini_firehoses(self):
        return list(self.mini_firehoses.keys())

    async def delete_mini_firehose(self, firehose_name: str):
        if firehose_name not in self.mini_firehoses:
            raise HTTPException(status_code=400, detail="MiniFirehose not found")
        self.mini_firehoses[firehose_name].stop()
        del self.mini_firehoses[firehose_name]
        return {"message": f"MiniFirehose '{firehose_name}' deleted"}

    async def add_message(self, firehose_name: str, message: MessageModel):
        if firehose_name not in self.mini_firehoses:
            raise HTTPException(status_code=400, detail="MiniFirehose not found")
        self.mini_firehoses[firehose_name].add_message(message.message)
        return {"message": "Message added"}

    async def get_stats(self, firehose_name: str):
        if firehose_name not in self.mini_firehoses:
            raise HTTPException(status_code=404, detail="MiniFirehose not found")
        # Implement your method to get stats from the firehose
        stats = {
            "buffer-count": self.mini_firehoses[firehose_name].buffer_count,
            "buffer-size-in-mb": self.mini_firehoses[firehose_name].buffer_size_in_mb
        }
        return stats


if __name__ == "__main__":
    mini_firehose_api = MiniFirehoseApi()
    mini_firehose_api.start()
    asyncio.run(mini_firehose_api.start())
    # To stop the server, you can call the stop method
    #mini_firehose_api.stop()
