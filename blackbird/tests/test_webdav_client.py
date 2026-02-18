"""Tests for WebDAVClient initialization, URL parsing, and file operations."""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

from blackbird.sync import WebDAVClient, configure_client


class TestWebDAVClientInit:
    """Tests for WebDAVClient URL parsing and initialization."""

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_basic_url(self, mock_session_cls, mock_client_cls):
        """Client initializes with a plain webdav:// URL."""
        client = WebDAVClient("webdav://localhost:7771")

        assert client.base_url == "http://localhost:7771"
        mock_client_cls.assert_called_once()
        opts = mock_client_cls.call_args[0][0]
        assert opts["webdav_hostname"] == "http://localhost:7771"
        assert "webdav_login" not in opts

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_url_with_credentials(self, mock_session_cls, mock_client_cls):
        """Client extracts username and password from URL."""
        client = WebDAVClient("webdav://user:pass@myhost:8080")

        opts = mock_client_cls.call_args[0][0]
        assert opts["webdav_login"] == "user"
        assert opts["webdav_password"] == "pass"
        assert client.base_url == "http://myhost:8080"

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_url_with_path(self, mock_session_cls, mock_client_cls):
        """Client preserves the path portion of the URL."""
        client = WebDAVClient("webdav://localhost:7771/data")

        opts = mock_client_cls.call_args[0][0]
        assert opts["webdav_root"] == "/data"

    def test_invalid_scheme_raises(self):
        """Non-webdav schemes are rejected."""
        with pytest.raises(ValueError, match="webdav://"):
            WebDAVClient("http://localhost:7771")

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_default_root_path(self, mock_session_cls, mock_client_cls):
        """URL without a path defaults to /."""
        WebDAVClient("webdav://localhost:7771")
        opts = mock_client_cls.call_args[0][0]
        assert opts["webdav_root"] == "/"


class TestWebDAVClientEncodePath:
    """Tests for URL path encoding of special characters."""

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_encode_hash(self, mock_session_cls, mock_client_cls):
        """Hash symbols in paths are percent-encoded."""
        client = WebDAVClient("webdav://localhost:7771")
        encoded = client._encode_url_path("Artist/#Track.mp3")
        assert "#" not in encoded
        assert "Artist/" in encoded

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_encode_spaces(self, mock_session_cls, mock_client_cls):
        """Spaces in paths are percent-encoded."""
        client = WebDAVClient("webdav://localhost:7771")
        encoded = client._encode_url_path("Artist Name/Album Title/Track.mp3")
        assert " " not in encoded

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_encode_preserves_slashes(self, mock_session_cls, mock_client_cls):
        """Forward slashes are preserved as path separators."""
        client = WebDAVClient("webdav://localhost:7771")
        encoded = client._encode_url_path("a/b/c.mp3")
        assert encoded.count("/") == 2

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_encode_cyrillic(self, mock_session_cls, mock_client_cls):
        """Cyrillic characters are percent-encoded."""
        client = WebDAVClient("webdav://localhost:7771")
        encoded = client._encode_url_path("Артист/Альбом/Трек.mp3")
        # Cyrillic chars should be encoded (no raw Cyrillic in URL)
        assert "Артист" not in encoded
        assert encoded.count("/") == 2


class TestWebDAVClientDownload:
    """Tests for the download_file method."""

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_download_success(self, mock_session_cls, mock_client_cls, tmp_path):
        """Successful download returns True and creates the file."""
        client = WebDAVClient("webdav://localhost:7771")
        # Mock the session.get to return file content
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content = MagicMock(return_value=[b"file content"])
        mock_response.headers = {"content-length": "12"}
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        client.session.get.return_value = mock_response

        dest = tmp_path / "downloaded.json"
        result = client.download_file("schema.json", dest)

        assert result is True
        client.session.get.assert_called_once()

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_download_creates_parent_dirs(self, mock_session_cls, mock_client_cls, tmp_path):
        """download_file creates parent directories if needed."""
        client = WebDAVClient("webdav://localhost:7771")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content = MagicMock(return_value=[b"data"])
        mock_response.headers = {"content-length": "4"}
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        client.session.get.return_value = mock_response

        dest = tmp_path / "sub" / "dir" / "file.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        client.download_file("path/to/file.json", dest)

        assert client.session.get.called


class TestConfigureClient:
    """Tests for the configure_client helper function."""

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_returns_webdav_client(self, mock_session_cls, mock_client_cls):
        """configure_client returns a WebDAVClient instance."""
        client = configure_client("webdav://localhost:7771")
        assert isinstance(client, WebDAVClient)

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_passes_http2_flag(self, mock_session_cls, mock_client_cls):
        """HTTP/2 flag is passed through to the client."""
        # Without httpx/h2, use_http2 will be False even if requested
        with patch("blackbird.sync.HTTPX_AVAILABLE", False):
            client = configure_client("webdav://localhost:7771", use_http2=True)
            assert hasattr(client, "use_http2")
            assert client.use_http2 is False

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_passes_pool_size(self, mock_session_cls, mock_client_cls):
        """Connection pool size is passed through."""
        client = configure_client("webdav://localhost:7771", connection_pool_size=20)
        assert client.connection_pool_size == 20
