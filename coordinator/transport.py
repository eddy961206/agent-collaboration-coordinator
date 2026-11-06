"""Allowlisted Gitea transport; TLS verifies peers, redirects/proxies are disabled."""
import base64
import json
import ssl
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class LabHTTP:
    def __init__(self, base, allowed_hosts, authorization=None, ca_file=None):
        parts = urlsplit(base)
        if (parts.scheme not in ('http', 'https') or parts.hostname not in allowed_hosts
                or parts.username or parts.password or parts.query or parts.fragment
                or parts.path not in ('', '/')):
            raise ValueError('lab_endpoint_required')
        self.base = base.rstrip('/')
        self.authorization = authorization
        context = ssl.create_default_context(cafile=ca_file)
        self.opener = build_opener(ProxyHandler({}), NoRedirect(), HTTPSHandler(context=context))

    def request(self, method, path, data=None):
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('absolute_endpoint_path_required')
        headers = {'Content-Type': 'application/json'}
        if self.authorization:
            headers['Authorization'] = self.authorization
        raw = json.dumps(data, ensure_ascii=False).encode() if data is not None else None
        request = Request(self.base + path, raw, headers, method=method)
        with self.opener.open(request, timeout=8) as response:
            body = response.read(2_000_001)
            if len(body) > 2_000_000:
                raise ValueError('response_too_large')
            return json.loads(body) if body else None


class GiteaClient:
    def __init__(self, base, allowed_hosts, user, password, repos, *, token=None, ca_file=None):
        if token is not None:
            if password:
                raise ValueError('choose_one_gitea_credential')
            authorization = 'token ' + token
        else:
            authorization = 'Basic ' + base64.b64encode(f'{user}:{password}'.encode()).decode()
        self.http = LabHTTP(base, allowed_hosts, authorization, ca_file=ca_file)
        self.repos = frozenset(repos)
        self.user = user

    def root(self, repo):
        if repo not in self.repos:
            raise ValueError('repository_not_allowed')
        return '/api/v1/repos/' + '/'.join(quote(p, safe='') for p in repo.split('/'))

    def issue(self, repo, number):
        return self.http.request('GET', self.root(repo) + f'/issues/{int(number)}')

    def comment(self, repo, comment_id):
        return self.http.request('GET', self.root(repo) + f'/issues/comments/{int(comment_id)}')

    def comments(self, repo, number):
        rows = []
        for page in range(1, 101):
            batch = self.http.request('GET', self.root(repo) + f'/issues/{int(number)}/comments?limit=50&page={page}')
            rows.extend(batch)
            if len(batch) < 50:
                return rows
        raise ValueError('comment_pagination_limit')

    def post(self, repo, number, body):
        return self.http.request('POST', self.root(repo) + f'/issues/{int(number)}/comments', {'body': body})

    def edit(self, repo, comment_id, body):
        return self.http.request('PATCH', self.root(repo) + f'/issues/comments/{int(comment_id)}', {'body': body})
