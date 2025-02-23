# WebDAV Setup Wizard Implementation Plan

## Module Structure

```
blackbird/
  webdav/
    __init__.py
    setup.py        # Main wizard implementation
    system_ops.py   # System-level operations (package installation, checks)
    config_gen.py   # Configuration generation and management
```

## CLI Interface

```bash
# Basic usage
blackbird webdav setup /path/to/dataset --port 7790

# With authentication
blackbird webdav setup /path/to/dataset --port 7790 --username user1 --password pass123

# List existing shares
blackbird webdav list

# Non-interactive mode (all options)
blackbird webdav setup /path/to/dataset --port 7790 --username user1 --password pass123 --non-interactive
```

## Implementation Details

### 1. CLI Integration (`cli.py`)

```python
@main.group()
def webdav():
    """WebDAV server management commands."""
    pass

@webdav.command()
@click.argument('dataset_path', type=click.Path(exists=True))
@click.option('--port', type=int, required=True, help='Port for WebDAV server')
@click.option('--username', help='WebDAV username')
@click.option('--password', help='WebDAV password')
@click.option('--non-interactive', is_flag=True, help='Run without prompts')
def setup(dataset_path: str, port: int, username: Optional[str], password: Optional[str], 
         non_interactive: bool):
    """Setup WebDAV server for dataset sharing."""
    wizard = WebDAVSetup(Path(dataset_path), port, username, password, non_interactive)
    wizard.run()

@webdav.command()
def list():
    """List WebDAV shares created by Blackbird."""
    WebDAVSetup.list_shares()
```

### 2. Main Setup Class (`setup.py`)

```python
class WebDAVSetup:
    def __init__(self, dataset_path: Path, port: int, username: Optional[str], 
                 password: Optional[str], non_interactive: bool):
        self.dataset_path = dataset_path
        self.port = port
        self.username = username
        self.password = password
        self.non_interactive = non_interactive
        self.config_name = f"blackbird-webdav-{port}"
        
    def run(self):
        """Main setup sequence."""
        try:
            # 1. Check system compatibility
            if not self._check_ubuntu():
                click.echo("Warning: This setup wizard is designed for Ubuntu systems")
                if not click.confirm("Continue anyway?"):
                    return

            # 2. Check and install dependencies
            if not self._ensure_dependencies():
                return

            # 3. Verify system resources
            self._check_system_resources()

            # 4. Get/verify credentials if needed
            if not self.non_interactive:
                self._prompt_credentials()

            # 5. Generate configuration
            config = self._generate_config()

            # 6. Show summary and confirm
            self._show_summary(config)
            if not self.non_interactive and not click.confirm("Apply these changes?"):
                return

            # 7. Apply configuration
            self._apply_config(config)

            # 8. Test configuration
            self._test_config()

            click.echo("WebDAV setup completed successfully!")
            
        except Exception as e:
            click.echo(f"Error during setup: {str(e)}", err=True)
            sys.exit(1)

    @staticmethod
    def list_shares():
        """List all Blackbird WebDAV shares."""
        shares = []
        config_dir = Path("/etc/nginx/sites-available")
        
        for config in config_dir.glob("blackbird-webdav-*.conf"):
            share = WebDAVShare.from_config(config)
            shares.append(share)
            
        if not shares:
            click.echo("No Blackbird WebDAV shares found")
            return
            
        click.echo("\nFound WebDAV shares:")
        for share in shares:
            click.echo(f"\nPort: {share.port}")
            click.echo(f"Path: {share.path}")
            click.echo(f"Status: {'Active' if share.is_running() else 'Inactive'}")
            click.echo(f"Config: {share.config_path}")
```

### 3. System Operations (`system_ops.py`)

```python
class SystemOps:
    @staticmethod
    def check_ubuntu() -> bool:
        """Check if running on Ubuntu."""
        return os.path.exists("/etc/lsb-release")

    @staticmethod
    def check_dependencies() -> Tuple[bool, List[str]]:
        """Check if nginx and webdav module are installed."""
        missing = []
        if not shutil.which("nginx"):
            missing.append("nginx")
        if not os.path.exists("/etc/nginx/modules-available/mod-http-dav-ext.conf"):
            missing.append("libnginx-mod-http-dav-ext")
        return not bool(missing), missing

    @staticmethod
    def install_dependencies(packages: List[str]) -> bool:
        """Install required packages."""
        try:
            subprocess.run(["sudo", "apt-get", "update"], check=True)
            subprocess.run(["sudo", "apt-get", "install", "-y"] + packages, check=True)
            return True
        except subprocess.CalledProcessError:
            return False

    @staticmethod
    def check_system_resources() -> Dict[str, bool]:
        """Check system resources for WebDAV."""
        return {
            "disk": psutil.disk_usage("/").free > 100 * 1024 * 1024,  # 100MB min
            "memory": psutil.virtual_memory().available > 50 * 1024 * 1024  # 50MB min
        }
```

### 4. Configuration Generation (`config_gen.py`)

```python
class ConfigGenerator:
    def __init__(self, dataset_path: Path, port: int, username: Optional[str] = None,
                 password: Optional[str] = None):
        self.dataset_path = dataset_path
        self.port = port
        self.username = username
        self.password = password
        
    def generate(self) -> str:
        """Generate nginx configuration for WebDAV."""
        auth_config = self._generate_auth_config() if self.username else ""
        
        return f"""
server {{
    listen {self.port};
    server_name _;
    
    root {self.dataset_path};
    
    location / {{
        dav_methods PUT DELETE MKCOL COPY MOVE;
        dav_ext_methods PROPFIND OPTIONS;
        
        # Read-only access
        limit_except GET PROPFIND OPTIONS {{
            deny all;
        }}
        
        {auth_config}
        
        # Directory listing
        autoindex on;
        
        # WebDAV configuration
        create_full_put_path on;
        dav_access user:r group:r all:r;
    }}
    
    access_log /var/log/nginx/webdav-{self.port}.access.log;
    error_log /var/log/nginx/webdav-{self.port}.error.log;
}}
"""

    def apply(self) -> bool:
        """Apply the configuration to the system."""
        config = self.generate()
        config_path = Path(f"/etc/nginx/sites-available/blackbird-webdav-{self.port}.conf")
        enabled_path = Path(f"/etc/nginx/sites-enabled/blackbird-webdav-{self.port}.conf")
        
        try:
            # Write configuration
            with open(config_path, 'w') as f:
                f.write(config)
                
            # Create symlink
            if enabled_path.exists():
                enabled_path.unlink()
            enabled_path.symlink_to(config_path)
            
            # Set up auth if needed
            if self.username:
                self._setup_auth()
                
            # Test and reload nginx
            if subprocess.run(["sudo", "nginx", "-t"]).returncode == 0:
                subprocess.run(["sudo", "systemctl", "reload", "nginx"])
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to apply configuration: {e}")
            return False
```

## Test Plan

### 1. WebDAV Setup Tests (`test_webdav_setup.py`)

```python
import pytest
import tempfile
import shutil
import os
import requests
from pathlib import Path
from blackbird.webdav.setup import WebDAVSetup
from blackbird.webdav.system_ops import SystemOps
from blackbird.webdav.config_gen import ConfigGenerator

@pytest.fixture
def webdav_setup():
    """Create a WebDAV setup instance with test configuration."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "test_dataset"
        dataset_path.mkdir(parents=True)
        
        # Create test files
        (dataset_path / "test_file.txt").write_text("Test content")
        (dataset_path / "test_dir").mkdir()
        (dataset_path / "test_dir" / "nested_file.txt").write_text("Nested content")
        
        setup = WebDAVSetup(
            dataset_path=dataset_path,
            port=8081,  # Use non-standard port for testing
            username="testuser",
            password="testpass",
            non_interactive=True
        )
        
        yield setup
        
        # Cleanup
        if os.path.exists("/etc/nginx/sites-enabled/blackbird-webdav-8081.conf"):
            setup._cleanup()

class TestWebDAVSystemOps:
    """Test system-level operations for WebDAV setup."""
    
    def test_check_ubuntu(self):
        """Test Ubuntu system detection."""
        is_ubuntu = SystemOps.check_ubuntu()
        assert isinstance(is_ubuntu, bool)
    
    def test_check_dependencies(self):
        """Test dependency checking."""
        installed, missing = SystemOps.check_dependencies()
        assert isinstance(installed, bool)
        assert isinstance(missing, list)
        assert all(isinstance(pkg, str) for pkg in missing)
    
    def test_check_system_resources(self):
        """Test system resource checking."""
        resources = SystemOps.check_system_resources()
        assert "disk" in resources
        assert "memory" in resources
        assert all(isinstance(v, bool) for v in resources.values())

class TestWebDAVConfig:
    """Test WebDAV configuration generation."""
    
    def test_generate_config_no_auth(self, webdav_setup):
        """Test generating nginx config without authentication."""
        config = ConfigGenerator(
            dataset_path=webdav_setup.dataset_path,
            port=8081
        ).generate()
        
        assert "listen 8081;" in config
        assert "root" in config
        assert "dav_methods" in config
        assert "auth_basic" not in config
    
    def test_generate_config_with_auth(self, webdav_setup):
        """Test generating nginx config with authentication."""
        config = ConfigGenerator(
            dataset_path=webdav_setup.dataset_path,
            port=8081,
            username="testuser",
            password="testpass"
        ).generate()
        
        assert "listen 8081;" in config
        assert "auth_basic" in config
        assert "auth_basic_user_file" in config

class TestWebDAVSetup:
    """Test complete WebDAV setup functionality."""
    
    def test_setup_without_auth(self, webdav_setup):
        """Test WebDAV setup without authentication."""
        # Modify setup for no auth
        webdav_setup.username = None
        webdav_setup.password = None
        
        # Run setup
        webdav_setup.run()
        
        # Verify server is accessible without auth
        response = requests.get(f"http://localhost:{webdav_setup.port}/test_file.txt")
        assert response.status_code == 200
        assert response.text == "Test content"
    
    def test_setup_with_auth(self, webdav_setup):
        """Test WebDAV setup with authentication."""
        # Run setup with auth
        webdav_setup.run()
        
        # Verify auth is required
        response = requests.get(f"http://localhost:{webdav_setup.port}/test_file.txt")
        assert response.status_code == 401
        
        # Verify correct auth works
        response = requests.get(
            f"http://localhost:{webdav_setup.port}/test_file.txt",
            auth=(webdav_setup.username, webdav_setup.password)
        )
        assert response.status_code == 200
        assert response.text == "Test content"
    
    def test_directory_listing(self, webdav_setup):
        """Test WebDAV directory listing functionality."""
        webdav_setup.run()
        
        response = requests.get(
            f"http://localhost:{webdav_setup.port}/",
            auth=(webdav_setup.username, webdav_setup.password)
        )
        assert response.status_code == 200
        assert "test_file.txt" in response.text
        assert "test_dir" in response.text
    
    def test_write_protection(self, webdav_setup):
        """Test write operations are properly blocked."""
        webdav_setup.run()
        
        # Try various write operations
        operations = [
            ("PUT", "/new_file.txt"),
            ("DELETE", "/test_file.txt"),
            ("MKCOL", "/new_dir"),
            ("MOVE", "/test_file.txt"),
            ("COPY", "/test_file.txt")
        ]
        
        for method, path in operations:
            response = requests.request(
                method,
                f"http://localhost:{webdav_setup.port}{path}",
                auth=(webdav_setup.username, webdav_setup.password)
            )
            assert response.status_code in (403, 405), f"{method} should be forbidden"
    
    def test_cleanup(self, webdav_setup):
        """Test proper cleanup of WebDAV configuration."""
        webdav_setup.run()
        config_path = Path(f"/etc/nginx/sites-enabled/blackbird-webdav-{webdav_setup.port}.conf")
        assert config_path.exists()
        
        webdav_setup._cleanup()
        assert not config_path.exists()
    
    def test_list_shares(self, webdav_setup):
        """Test listing of WebDAV shares."""
        webdav_setup.run()
        
        shares = WebDAVSetup.list_shares()
        assert len(shares) >= 1
        
        share = next(s for s in shares if s.port == webdav_setup.port)
        assert share.path == str(webdav_setup.dataset_path)
        assert share.is_running()
```

## Implementation Order

1. Create module structure
2. Implement `system_ops.py` for basic system checks
3. Implement `config_gen.py` for nginx configuration
4. Implement main `setup.py` class
5. Add CLI integration
6. Write tests
7. Manual testing and refinement

## Dependencies

- `nginx`
- `libnginx-mod-http-dav-ext`
- `python-requests` (for testing)
- `pytest` (for testing)

## Security Considerations

1. File permissions set to read-only for web access
2. Basic authentication support
3. Secure password storage using nginx's encrypted password files
4. Configuration files owned by root
5. Logs for access tracking

## Error Handling

1. Graceful handling of missing sudo privileges
2. Clear error messages for missing dependencies
3. Validation of all user inputs
4. Proper cleanup on failure
5. Detailed logging for troubleshooting 