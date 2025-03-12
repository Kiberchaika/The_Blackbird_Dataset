"""Main WebDAV setup functionality."""

import os
import sys
import click
from pathlib import Path
from typing import Optional, List, Dict
import logging
from dataclasses import dataclass

from .system_ops import SystemOps
from .config_gen import ConfigGenerator

logger = logging.getLogger(__name__)

@dataclass
class WebDAVShare:
    """Information about a WebDAV share."""
    port: int
    path: str
    config_path: str
    enabled: bool

    @classmethod
    def from_config(cls, config_path: Path) -> 'WebDAVShare':
        """Create share info from config file.
        
        Args:
            config_path: Path to nginx config file
            
        Returns:
            WebDAVShare instance
        """
        try:
            # Read config file
            with open(config_path) as f:
                content = f.read()
            
            # Extract port and path
            port = None
            path = None
            for line in content.split('\n'):
                if 'listen' in line:
                    port = int(line.split(';')[0].split()[-1])
                elif 'root' in line:
                    path = line.split(';')[0].split()[-1]
                    
            if not port or not path:
                raise ValueError("Invalid config file")
                
            # Check if enabled
            enabled_path = Path("/etc/nginx/sites-enabled") / config_path.name
            enabled = enabled_path.exists()
            
            return cls(
                port=port,
                path=path,
                config_path=str(config_path),
                enabled=enabled
            )
            
        except Exception as e:
            logger.error(f"Failed to parse config {config_path}: {e}")
            return None
            
    def is_running(self) -> bool:
        """Check if this share is active.
        
        Returns:
            bool: True if share is running
        """
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', self.port))
            sock.close()
            return result == 0
        except:
            return False

class WebDAVSetup:
    """WebDAV setup wizard."""
    
    def __init__(self, dataset_path: Path, port: int, username: Optional[str] = None,
                 password: Optional[str] = None, non_interactive: bool = False):
        """Initialize WebDAV setup.
        
        Args:
            dataset_path: Path to dataset directory
            port: Port number for WebDAV server
            username: Optional username for basic auth
            password: Optional password for basic auth
            non_interactive: Whether to run without prompts
        """
        self.dataset_path = Path(dataset_path)
        self.port = port
        self.username = username
        self.password = password
        self.non_interactive = non_interactive
        
    def _check_ubuntu(self) -> bool:
        """Check if running on Ubuntu."""
        return SystemOps.check_ubuntu()
        
    def _ensure_dependencies(self) -> bool:
        """Check and install dependencies.
        
        Returns:
            bool: True if all dependencies are available
        """
        installed, missing = SystemOps.check_dependencies()
        if not installed:
            if self.non_interactive:
                logger.error(f"Missing dependencies: {', '.join(missing)}")
                return False
                
            if not click.confirm(f"Install missing packages? ({', '.join(missing)})"):
                return False
                
            if not SystemOps.install_dependencies(missing):
                logger.error("Failed to install dependencies")
                return False
                
        return True
        
    def _check_system_resources(self) -> bool:
        """Check system resources.
        
        Returns:
            bool: True if system has sufficient resources
        """
        resources = SystemOps.check_system_resources()
        
        if not all(resources.values()):
            issues = [name for name, ok in resources.items() if not ok]
            logger.warning(f"Insufficient system resources: {', '.join(issues)}")
            
            if self.non_interactive:
                return False
                
            return click.confirm("Continue anyway?")
            
        return True
        
    def _prompt_credentials(self) -> None:
        """Prompt for auth credentials if needed."""
        if not self.username and click.confirm("Set up authentication?"):
            self.username = click.prompt("Username")
            self.password = click.prompt("Password", hide_input=True)
            
    def _generate_config(self) -> ConfigGenerator:
        """Generate nginx configuration.
        
        Returns:
            ConfigGenerator instance
        """
        return ConfigGenerator(
            dataset_path=self.dataset_path,
            port=self.port,
            username=self.username,
            password=self.password
        )
        
    def _show_summary(self, config: ConfigGenerator) -> None:
        """Show configuration summary."""
        click.echo("\nWebDAV Configuration Summary:")
        click.echo(f"Dataset path: {self.dataset_path}")
        click.echo(f"Port: {self.port}")
        click.echo(f"Authentication: {'Yes' if self.username else 'No'}")
        if self.username:
            click.echo(f"Username: {self.username}")
            
    def _cleanup(self) -> None:
        """Clean up configuration."""
        config = self._generate_config()
        config.remove()
        
    def run(self) -> bool:
        """Run the setup wizard.
        
        Returns:
            bool: True if setup successful
        """
        try:
            # Create dataset directory if it doesn't exist
            if not self.dataset_path.exists():
                logger.info(f"Creating dataset directory: {self.dataset_path}")
                self.dataset_path.mkdir(parents=True, exist_ok=True)

            # 1. Check system compatibility
            if not self._check_ubuntu():
                click.echo("Warning: This setup wizard is designed for Ubuntu systems")
                if not self.non_interactive and not click.confirm("Continue anyway?"):
                    return False

            # 2. Check and install dependencies
            if not self._ensure_dependencies():
                return False

            # 3. Verify system resources
            if not self._check_system_resources():
                return False

            # 4. Get/verify credentials if needed
            if not self.non_interactive:
                self._prompt_credentials()

            # 5. Generate configuration
            config = self._generate_config()

            # 6. Show summary and confirm
            self._show_summary(config)
            if not self.non_interactive and not click.confirm("Apply these changes?"):
                return False

            # 7. Apply configuration
            if not config.apply():
                logger.error("Failed to apply configuration")
                return False

            # 8. Setup permissions and configure firewall
            logger.info("Setting up permissions and configuring firewall...")
            if not SystemOps.setup_permissions_and_firewall(str(self.dataset_path), port=self.port, password=self.password, non_interactive=self.non_interactive):
                logger.error("Failed to setup permissions and firewall")
                return False

            click.echo("WebDAV setup completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Error during setup: {e}")
            return False
            
    @staticmethod
    def list_shares() -> List[WebDAVShare]:
        """List all Blackbird WebDAV shares.
        
        Returns:
            List[WebDAVShare]: List of active shares
        """
        shares = []
        config_dir = Path("/etc/nginx/sites-available")
        
        if not config_dir.exists():
            return shares
            
        for config in config_dir.glob("blackbird-webdav-*.conf"):
            share = WebDAVShare.from_config(config)
            if share:
                shares.append(share)
                
        return shares 