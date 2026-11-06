import ssl
import unittest
from urllib.request import HTTPSHandler

from coordinator.transport import GiteaClient, LabHTTP


class TransportTests(unittest.TestCase):
    def test_https_requires_peer_and_hostname_verification(self):
        client = LabHTTP('https://gitea.example', ['gitea.example'])
        handler = next(h for h in client.opener.handlers if isinstance(h, HTTPSHandler))
        self.assertEqual(ssl.CERT_REQUIRED, handler._context.verify_mode)
        self.assertTrue(handler._context.check_hostname)

    def test_endpoint_userinfo_and_unapproved_host_are_rejected(self):
        for url in ['https://other.example', 'https://secret@gitea.example', 'file:///tmp/config', 'https://gitea.example/?token=secret']:
            with self.assertRaises(ValueError):
                LabHTTP(url, ['gitea.example'])

    def test_credentials_are_not_in_url_and_cannot_be_ambiguous(self):
        client = GiteaClient('https://gitea.example', ['gitea.example'], 'bot', '', ['fake/team'], token='fixture')
        self.assertEqual('token fixture', client.http.authorization)
        self.assertNotIn('fixture', client.http.base)
        with self.assertRaises(ValueError):
            GiteaClient('https://gitea.example', ['gitea.example'], 'bot', 'password', ['fake/team'], token='fixture')
