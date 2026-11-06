"""Authenticated worker RPC; handles survive coordinator process replacement."""
import hashlib
import hmac
import ipaddress
import json
import re
import ssl
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .adapters import AdapterError, snapshot_payload


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def launch_spec(run):
    fields = ('id', 'task_id', 'owner', 'adapter', 'revision', 'generation', 'text')
    value = {key: run[key] for key in fields}
    if not isinstance(value['id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value['id']):
        raise AdapterError('invalid_run_id')
    for key in ('task_id', 'owner', 'adapter'):
        if not isinstance(value[key], str) or not value[key] or len(value[key]) > 512:
            raise AdapterError('invalid_launch_identity')
    for key in ('revision', 'generation'):
        if not isinstance(value[key], int) or isinstance(value[key], bool) or value[key] < 1:
            raise AdapterError('invalid_launch_version')
    if not isinstance(value['text'], str) or len(value['text']) > 500_000:
        raise AdapterError('invalid_launch_text')
    if run.get('resume_from') is not None:
        prior = run['resume_from']
        if not isinstance(prior, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', prior):
            raise AdapterError('invalid_resume_source')
        value['resume_from'] = prior
    return value


def handle_for(run, node_id, token, engine, ledger_id):
    spec = launch_spec(run)
    binding = hashlib.sha256(canonical_json(spec).encode()).hexdigest()
    handle = {'adapter': 'remote', 'node_id': node_id, 'ledger_id': ledger_id, 'engine': engine,
              'run_id': spec['id'], 'binding': binding}
    handle['proof'] = hmac.new(token.encode(), canonical_json(handle).encode(), hashlib.sha256).hexdigest()
    return handle


def verify_handle(handle, node_id, token, engine=None, ledger_id=None):
    if not isinstance(handle, dict) or set(handle) != {'adapter', 'node_id', 'ledger_id', 'engine', 'run_id', 'binding', 'proof'}:
        raise AdapterError('invalid_remote_handle')
    if handle['adapter'] != 'remote' or handle['node_id'] != node_id or (engine and handle['engine'] != engine):
        raise AdapterError('remote_identity_mismatch')
    if not isinstance(handle['ledger_id'], str) or not re.fullmatch(r'[a-f0-9]{32}', handle['ledger_id']):
        raise AdapterError('invalid_remote_handle')
    if ledger_id is not None and handle['ledger_id'] != ledger_id:
        raise AdapterError('remote_ledger_changed')
    if not isinstance(handle['run_id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', handle['run_id']):
        raise AdapterError('invalid_remote_handle')
    unsigned = {key: value for key, value in handle.items() if key != 'proof'}
    proof = hmac.new(token.encode(), canonical_json(unsigned).encode(), hashlib.sha256).hexdigest()
    if not isinstance(handle['proof'], str) or not hmac.compare_digest(handle['proof'], proof):
        raise AdapterError('remote_handle_proof_invalid')


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _loopback(host):
    if host == 'localhost':
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except (ValueError, TypeError):
        return False


class RemoteAdapter:
    """A coordinator-side adapter with no in-memory process ownership registry."""
    durable_remote = True

    def __init__(self, base_url, node_id, token, engine, allowed_owners, timeout=15, ca_file=None):
        parsed = urlsplit(base_url)
        if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/')):
            raise AdapterError('worker_endpoint_invalid')
        if parsed.scheme == 'http' and not _loopback(parsed.hostname):
            raise AdapterError('worker_requires_ssh_tunnel_or_tls')
        if not node_id or not token or not engine:
            raise AdapterError('worker_configuration_missing')
        self.base_url = base_url.rstrip('/')
        self.node_id, self.token, self.engine = node_id, token, engine
        self.allowed_owners = frozenset(allowed_owners)
        if not self.allowed_owners:
            raise AdapterError('worker_owner_allowlist_empty')
        self.timeout = timeout
        self.opener = build_opener(ProxyHandler({}), _NoRedirect(),
                                   HTTPSHandler(context=ssl.create_default_context(cafile=ca_file)))

    def prepare(self, run):
        if run.get('owner') not in self.allowed_owners:
            raise AdapterError('worker_owner_not_allowed')
        ledger_id = self.info()['ledger_id']
        return handle_for(run, self.node_id, self.token, self.engine, ledger_id)

    def _request(self, path, body=None, handle=None):
        headers = {'Authorization': 'Bearer ' + self.token, 'X-Worker-Node': self.node_id,
                   'Content-Type': 'application/json'}
        raw = canonical_json(body).encode() if body is not None else None
        request = Request(self.base_url + path, raw, headers, method='POST' if body is not None else 'GET')
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                data = response.read(4_000_001)
                if len(data) > 4_000_000:
                    raise AdapterError('worker_response_too_large')
                result = json.loads(data)
        except HTTPError as exc:
            try:
                result = json.loads(exc.read(4096))
            except (ValueError, OSError):
                result = {}
            finally:
                exc.close()
            if result.get('node_id') != self.node_id or not result.get('instance_id'):
                raise AdapterError('worker_response_identity_mismatch') from exc
            if handle is not None and result.get('ledger_id') != handle['ledger_id']:
                raise AdapterError('remote_ledger_changed') from exc
            code = result.get('error')
            if not isinstance(code, str) or not re.fullmatch(r'[a-z0-9_]{1,80}', code):
                code = 'worker_http_' + str(exc.code)
            raise AdapterError(code) from exc
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            raise AdapterError('worker_unreachable') from exc
        if (not isinstance(result, dict) or result.get('node_id') != self.node_id or not result.get('instance_id')
                or not isinstance(result.get('ledger_id'), str) or not re.fullmatch(r'[a-f0-9]{32}', result['ledger_id'])):
            raise AdapterError('worker_response_identity_mismatch')
        if handle is not None:
            if result['ledger_id'] != handle['ledger_id']:
                raise AdapterError('remote_ledger_changed')
            verify_handle(result.get('handle'), self.node_id, self.token, self.engine)
            if result['handle'] != handle:
                raise AdapterError('worker_response_handle_mismatch')
        return result

    def info(self):
        return self._request('/v1/info')

    def start(self, run):
        handle = self.prepare(run)
        return self.start_prepared(run, handle)

    def start_prepared(self, run, handle):
        verify_handle(handle, self.node_id, self.token, self.engine)
        if run.get('owner') not in self.allowed_owners:
            raise AdapterError('worker_owner_not_allowed')
        if handle != handle_for(run, self.node_id, self.token, self.engine, handle['ledger_id']):
            raise AdapterError('worker_launch_binding_mismatch')
        body = {'handle': handle, 'run': launch_spec(run), 'operation_id': 'start:' + run['id']}
        try:
            self._request('/v1/start', body, handle)
        except AdapterError as exc:
            exc.handle = handle
            raise
        return handle

    def deliver(self, handle, snapshot):
        verify_handle(handle, self.node_id, self.token, self.engine)
        payload = snapshot_payload(snapshot)
        operation_id = 'deliver:' + handle['run_id'] + ':' + hashlib.sha256(canonical_json(payload).encode()).hexdigest()
        result = self._request('/v1/deliver', {'handle': handle, 'snapshot': payload,
                                             'operation_id': operation_id}, handle)
        return result['receipt']

    def stop(self, handle):
        verify_handle(handle, self.node_id, self.token, self.engine)
        result = self._request('/v1/stop', {'handle': handle, 'operation_id': 'stop:' + handle['run_id']}, handle)
        return result['observation']

    def observe(self, handle):
        verify_handle(handle, self.node_id, self.token, self.engine)
        result = self._request('/v1/observe', {'handle': handle}, handle)
        observation = result['observation']
        if not isinstance(observation, dict):
            raise AdapterError('invalid_worker_observation')
        return {**observation, 'node_id': self.node_id, 'node_instance_id': result['instance_id'],
                'ledger_id': result['ledger_id'], 'run_id': handle['run_id'], 'binding': handle['binding']}

    def recover(self, handle):
        return self.observe(handle)

    def close(self):
        # A controller disconnect must not kill jobs owned by the worker host.
        return None
