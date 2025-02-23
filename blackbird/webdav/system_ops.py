"""System-level operations for WebDAV setup."""

import os
import shutil
import subprocess
import psutil
from typing import Tuple, List, Dict, Optional
import logging
import getpass

logger = logging.getLogger(__name__)

class SystemOps:
    """System operations for WebDAV setup."""

    @staticmethod
    def check_ubuntu() -> bool:
        """Check if running on Ubuntu.
        
        Returns:
            bool: True if running on Ubuntu, False otherwise
        """
        try:
            with open("/etc/lsb-release") as f:
                return "Ubuntu" in f.read()
        except (FileNotFoundError, IOError):
            return False

    @staticmethod
    def check_dependencies() -> Tuple[bool, List[str]]:
        """Check if required packages are installed.
        
        Returns:
            Tuple[bool, List[str]]: (all_installed, missing_packages)
        """
        required_packages = ["nginx", "libnginx-mod-http-dav-ext"]
        missing = []
        
        try:
            # Check each package using dpkg
            for package in required_packages:
                result = subprocess.run(
                    ["dpkg", "-l", package], 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    text=True
                )
                if "ii" not in result.stdout:  # 'ii' indicates package is installed
                    missing.append(package)
        except Exception as e:
            logger.warning(f"Failed to check package status: {e}")
            # Fall back to basic checks if dpkg fails
            if not shutil.which("nginx"):
                missing.append("nginx")
            if not os.path.exists("/etc/nginx/modules-available/mod-http-dav-ext.conf"):
                missing.append("libnginx-mod-http-dav-ext")
        
        return not bool(missing), missing

    @staticmethod
    def run_with_sudo(cmd: List[str], password: Optional[str] = None, non_interactive: bool = False) -> bool:
        """Run command with sudo, optionally using provided password.
        
        Args:
            cmd: Command to run
            password: Optional sudo password
            non_interactive: Whether to run in non-interactive mode
            
        Returns:
            bool: True if command succeeded
        """
        try:
            # First try without password
            result = subprocess.run(["sudo", "-n"] + cmd, capture_output=True)
            if result.returncode == 0:
                return True

            # If that fails and we're in non-interactive mode, fail
            if non_interactive:
                logger.error("Sudo access required but running in non-interactive mode")
                return False

            # If no password provided, ask for it
            if password is None:
                password = getpass.getpass("Sudo password required: ")

            # Run with password
            process = subprocess.Popen(
                ["sudo", "-S"] + cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=password + '\n')
            
            if process.returncode != 0:
                logger.error(f"Command failed: {stderr}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to run command: {e}")
            return False

    @staticmethod
    def install_dependencies(packages: List[str], password: Optional[str] = None, non_interactive: bool = False) -> bool:
        """Install required packages.
        
        Args:
            packages: List of package names to install
            password: Optional sudo password
            non_interactive: Whether to run in non-interactive mode
            
        Returns:
            bool: True if installation successful, False otherwise
        """
        try:
            # Update package list
            logger.info("Updating package list...")
            if not SystemOps.run_with_sudo(["apt-get", "update"], password, non_interactive):
                return False

            # Install packages
            logger.info(f"Installing packages: {', '.join(packages)}")
            if not SystemOps.run_with_sudo(["apt-get", "install", "-y"] + packages, password, non_interactive):
                return False

            return True
            
        except Exception as e:
            logger.error(f"Failed to install packages: {e}")
            return False

    @staticmethod
    def check_system_resources() -> Dict[str, bool]:
        """Check system resources for WebDAV operation.
        
        Returns:
            Dict[str, bool]: Resource check results
        """
        try:
            # Required minimums
            MIN_DISK_SPACE = 100 * 1024 * 1024  # 100MB
            MIN_MEMORY = 50 * 1024 * 1024      # 50MB
            
            # Check disk space
            disk_usage = psutil.disk_usage("/")
            disk_ok = disk_usage.free >= MIN_DISK_SPACE
            
            # Check memory
            memory = psutil.virtual_memory()
            memory_ok = memory.available >= MIN_MEMORY
            
            return {
                "disk": disk_ok,
                "memory": memory_ok
            }
            
        except Exception as e:
            logger.error(f"Failed to check system resources: {e}")
            return {
                "disk": False,
                "memory": False
            } 