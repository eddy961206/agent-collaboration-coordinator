"""Deterministic local Messages/Responses fixture. It never calls another server."""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time
import uuid


class FixtureServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, output_dir):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.requests = []
        self.lock = threading.Lock()
        self.release = threading.Event()
        self.hold = False
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status); self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)

    def do_GET(self):
        if self.path == '/fixture/requests':
            return self.respond(200, self.server.requests)
        if self.path == '/health':
            return self.respond(200, {'fake_provider': True, 'model_calls': 0})
        if self.path.startswith('/v1/models'):
            return self.respond(200, {'object': 'list', 'data': []})
        return self.respond(404, {'error': 'unsupported_fixture_path'})

    def do_POST(self):
        length = int(self.headers.get('Content-Length', '0'))
        if not 0 <= length < 10_000_000:
            return self.respond(413, {'error': 'too_large'})
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except ValueError:
            return self.respond(400, {'error': 'fixture_requires_uncompressed_json', 'encoding': self.headers.get('Content-Encoding')})
        if self.path == '/fixture/control':
            self.server.hold = bool(body.get('hold'))
            if self.server.hold:
                self.server.release.clear()
            else:
                self.server.release.set()
            return self.respond(200, {'hold': self.server.hold})
        if not (self.path.startswith('/v1/messages') or self.path.rstrip('/') == '/v1/responses'):
            return self.respond(404, {'error': 'unsupported_fixture_path'})
        row = {'path': self.path, 'body': body, 'time': time.time(), 'model_calls': 0}
        with self.server.lock:
            self.server.requests.append(row)
            with (self.server.output_dir / 'model-requests.jsonl').open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(row, ensure_ascii=False) + '\n')
        if self.path.endswith('/count_tokens'):
            return self.respond(200, {'input_tokens': 0})
        if self.server.hold:
            self.server.release.wait(timeout=120)
        identity = uuid.uuid4().hex
        text = 'NATIVE_FIXTURE_RESPONSE: no model was called.'
        if self.path.startswith('/v1/messages'):
            response = {'id': 'msg_' + identity, 'type': 'message', 'role': 'assistant', 'model': body.get('model'),
                        'content': [{'type': 'text', 'text': text}], 'stop_reason': 'end_turn', 'stop_sequence': None,
                        'usage': {'input_tokens': 0, 'output_tokens': 0}}
            if not body.get('stream'):
                return self.respond(200, response)
            events = [
                ('message_start', {'type': 'message_start', 'message': {**response, 'content': [], 'stop_reason': None}}),
                ('content_block_start', {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}}),
                ('content_block_delta', {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': text}}),
                ('content_block_stop', {'type': 'content_block_stop', 'index': 0}),
                ('message_delta', {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn', 'stop_sequence': None}, 'usage': {'output_tokens': 0}}),
                ('message_stop', {'type': 'message_stop'}),
            ]
        else:
            rid = 'resp_' + identity
            events = [
                ('response.created', {'type': 'response.created', 'response': {'id': rid}}),
                ('response.output_item.done', {'type': 'response.output_item.done', 'item': {'id': 'msg_' + identity,
                    'type': 'message', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': text}]}}),
                ('response.completed', {'type': 'response.completed', 'response': {'id': rid, 'usage': {
                    'input_tokens': 0, 'input_tokens_details': None, 'output_tokens': 0, 'output_tokens_details': None, 'total_tokens': 0}}}),
            ]
        self.send_response(200); self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Connection', 'close'); self.end_headers()
        try:
            for event, value in events:
                self.wfile.write(('event: ' + event + '\ndata: ' + json.dumps(value) + '\n\n').encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--listen', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=19091)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    server = FixtureServer((args.listen, args.port), args.output)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
