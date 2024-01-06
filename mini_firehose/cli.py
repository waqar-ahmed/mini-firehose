import argparse
import asyncio
from mini_firehose.api import MiniFirehoseApi  # Import the FastAPIServer from your api.py


class MiniFirehoseCLI:
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='mini-firehose', description='Mini Firehose CLI Tool')
        self.api = None
        self._setup_parser()

    def _setup_parser(self):
        subparsers = self.parser.add_subparsers(dest='command')

        # Subparser for 'api' command
        api_parser = subparsers.add_parser("api", help="Manage Mini Firehose API")

        # Start command
        start_parser = api_parser.add_subparsers(dest='api_command')
        start_cmd = start_parser.add_parser('start', help='Start the Mini Firehose server')
        start_cmd.set_defaults(func=self.start_server)
        start_cmd.add_argument('--host', type=str, default='127.0.0.1', help='Host for the server')
        start_cmd.add_argument('--port', type=int, default=8000, help='Port for the server')

        # Stop command
        stop_cmd = start_parser.add_parser('stop', help='Stop the Mini Firehose server')
        stop_cmd.set_defaults(func=self.stop_server)

    def start_server(self, args):
        if self.api is None:
            self.api = MiniFirehoseApi(args.host, args.port)
            print(f"Server starting at {args.host}:{args.port}")
            asyncio.run(self.api.start())

    def stop_server(self, args):
        if self.api:
            self.api.stop()
            print("Server stopped.")
        else:
            print("Server is not running.")

    def run(self):
        args = self.parser.parse_args()
        if hasattr(args, 'func'):
            args.func(args)
        else:
            self.parser.print_help()


def main():
    cli = MiniFirehoseCLI()
    cli.run()


if __name__ == '__main__':
    main()
