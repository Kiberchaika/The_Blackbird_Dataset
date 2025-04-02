"""Configuration generation for WebDAV setup."""

import os
import subprocess
from pathlib import Path
from typing import Optional
import logging
from passlib.apache import HtpasswdFile

logger = logging.getLogger(__name__)

class ConfigGenerator:
    """Generates and manages nginx WebDAV configurations."""

    def __init__(self, dataset_path: Path, port: int, username: Optional[str] = None,
                 password: Optional[str] = None):
        """Initialize configuration generator.
        
        Args:
            dataset_path: Path to dataset directory
            port: Port number for WebDAV server
            username: Optional username for basic auth
            password: Optional password for basic auth
        """
        self.dataset_path = Path(dataset_path)
        self.port = port
        self.username = username
        self.password = password
        self.config_name = f"blackbird-webdav-{port}"
        
    def _generate_auth_config(self) -> str:
        """Generate nginx auth configuration.
        
        Returns:
            str: Nginx auth configuration block
        """
        if not self.username or not self.password:
            return ""
            
        auth_file = f"/etc/nginx/.htpasswd_{self.config_name}"
        return f"""
        auth_basic "Blackbird WebDAV";
        auth_basic_user_file {auth_file};
        """
        
    def _setup_auth(self) -> bool:
        """Set up authentication configuration.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.username or not self.password:
            return True
            
        try:
            # Create temporary htpasswd file
            temp_auth_file = Path(f"/tmp/.htpasswd_{self.config_name}")
            ht = HtpasswdFile(str(temp_auth_file), new=True)
            ht.set_password(self.username, self.password)
            ht.save()
            
            # Move file to nginx directory with sudo
            auth_file = f"/etc/nginx/.htpasswd_{self.config_name}"
            subprocess.run(["sudo", "mv", str(temp_auth_file), auth_file], check=True)
            
            # Set proper permissions
            subprocess.run(["sudo", "chown", "www-data:www-data", auth_file], check=True)
            subprocess.run(["sudo", "chmod", "600", auth_file], check=True)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to set up authentication: {e}")
            return False
        
    def generate(self) -> str:
        """Generate nginx configuration for WebDAV.
        
        Returns:
            str: Complete nginx configuration
        """
        auth_config = self._generate_auth_config() if self.username else ""
        
        return f"""
server {{
    listen {self.port};
    server_name _;
    
    root {self.dataset_path.absolute()};
    client_max_body_size 0;
    
    location / {{
        # WebDAV methods
        dav_methods PUT DELETE MKCOL COPY MOVE;
        dav_ext_methods PROPFIND OPTIONS;
        
        # Read-only access
        limit_except GET PROPFIND OPTIONS {{
            deny all;
        }}
        
        # Allow directory listing
        autoindex on;
        
        # WebDAV configuration
        create_full_put_path on;
        dav_access user:rw group:rw all:r;
        
        {auth_config}
    }}
    
    access_log /var/log/nginx/webdav-{self.port}.access.log;
    error_log /var/log/nginx/webdav-{self.port}.error.log;
}}
"""

    def apply(self) -> bool:
        """Apply the configuration to the system.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure nginx is running
            status = subprocess.run(["systemctl", "is-active", "nginx"], capture_output=True, text=True)
            if status.stdout.strip() != "active":
                logger.info("Starting nginx service...")
                if subprocess.run(["sudo", "systemctl", "start", "nginx"]).returncode != 0:
                    logger.error("Failed to start nginx service")
                    return False

            # Generate config
            config = self.generate()
            config_path = Path(f"/etc/nginx/sites-available/{self.config_name}.conf")
            enabled_path = Path(f"/etc/nginx/sites-enabled/{self.config_name}.conf")
            
            # Write configuration with sudo
            cmd = ["sudo", "tee", str(config_path)]
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)
            process.communicate(input=config)
            
            if process.returncode != 0:
                logger.error("Failed to write configuration file")
                return False
            
            # Create symlink if it doesn't exist
            if not enabled_path.exists():
                subprocess.run(["sudo", "ln", "-s", str(config_path), str(enabled_path)], check=True)
            
            # Set up auth if needed
            if self.username and not self._setup_auth():
                return False
            
            # Test and reload nginx
            if subprocess.run(["sudo", "nginx", "-t"], capture_output=True).returncode != 0:
                logger.error("Nginx configuration test failed")
                return False
                
            if subprocess.run(["sudo", "systemctl", "reload", "nginx"]).returncode != 0:
                logger.error("Failed to reload nginx")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply configuration: {e}")
            return False
            
    def remove(self) -> bool:
        """Remove the configuration from the system.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            config_path = Path(f"/etc/nginx/sites-available/{self.config_name}.conf")
            enabled_path = Path(f"/etc/nginx/sites-enabled/{self.config_name}.conf")
            auth_file = Path(f"/etc/nginx/.htpasswd_{self.config_name}")
            
            # Remove files if they exist
            for path in [enabled_path, config_path, auth_file]:
                if path.exists():
                    subprocess.run(["sudo", "rm", str(path)], check=True)
            
            # Reload nginx
            subprocess.run(["sudo", "systemctl", "reload", "nginx"], check=True)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove configuration: {e}")
            return False 