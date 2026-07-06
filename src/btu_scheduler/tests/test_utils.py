"""
Unit tests for utility functions in btu_scheduler.lib.utils.

No running services required.
"""

import unittest
from unittest.mock import MagicMock


def _mock_config(token="Token abc123", host_header=None):
	config = MagicMock()
	config.webserver_token = token
	config.webserver_host_header = host_header
	return config


class TestBuildFrappeHeaders(unittest.TestCase):

	def setUp(self):
		from btu_scheduler.lib.utils import build_frappe_headers
		self.build = build_frappe_headers

	def test_returns_dict(self):
		self.assertIsInstance(self.build(_mock_config()), dict)

	def test_authorization_is_token(self):
		headers = self.build(_mock_config(token="Token mytoken"))
		self.assertEqual(headers["Authorization"], "Token mytoken")

	def test_default_content_type_is_json(self):
		headers = self.build(_mock_config())
		self.assertEqual(headers["Content-Type"], "application/json")

	def test_custom_content_type_is_passed_through(self):
		headers = self.build(_mock_config(), content_type="application/octet-stream")
		self.assertEqual(headers["Content-Type"], "application/octet-stream")

	def test_no_host_header_when_not_configured(self):
		headers = self.build(_mock_config(host_header=None))
		self.assertNotIn("Host", headers)

	def test_host_header_added_when_configured(self):
		headers = self.build(_mock_config(host_header="mysite.example.com"))
		self.assertEqual(headers["Host"], "mysite.example.com")

	def test_empty_string_host_header_not_added(self):
		# Falsy host_header should not produce a Host key
		headers = self.build(_mock_config(host_header=""))
		self.assertNotIn("Host", headers)

	def test_three_keys_without_host_header(self):
		headers = self.build(_mock_config(host_header=None))
		self.assertEqual(set(headers.keys()), {"Authorization", "Content-Type"})

	def test_four_keys_with_host_header(self):
		headers = self.build(_mock_config(host_header="site.example.com"))
		self.assertEqual(set(headers.keys()), {"Authorization", "Content-Type", "Host"})


if __name__ == "__main__":
	unittest.main()
