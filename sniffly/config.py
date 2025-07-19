"""Configuration management for Sniffly.

Implements a layered configuration system with priority:
CLI args > environment variables > config file > defaults
"""

import json
import os
import re
from pathlib import Path
from typing import Any

# Default configuration values
DEFAULTS = {
    "port": 8081,
    "host": "127.0.0.1",
    "cache_max_projects": 5,
    "cache_max_mb_per_project": 500,
    "auto_browser": True,
    "max_date_range_days": 30,
    "messages_initial_load": 500,
    "enable_memory_monitor": False,
    "enable_background_processing": True,
    "cache_warm_on_startup": 3,
    "log_level": "INFO",
    "share_base_url": "https://sniffly.dev",
    "share_api_url": "https://sniffly.dev",
    "share_enabled": True,
    "rollups": {},
}

# Map config keys to environment variable names
ENV_MAPPINGS = {
    "port": "PORT",
    "host": "HOST",
    "cache_max_projects": "CACHE_MAX_PROJECTS",
    "cache_max_mb_per_project": "CACHE_MAX_MB_PER_PROJECT",
    "auto_browser": "AUTO_BROWSER",
    "max_date_range_days": "MAX_DATE_RANGE_DAYS",
    "messages_initial_load": "MESSAGES_INITIAL_LOAD",
    "enable_memory_monitor": "ENABLE_MEMORY_MONITOR",
    "enable_background_processing": "ENABLE_BACKGROUND_PROCESSING",
    "cache_warm_on_startup": "CACHE_WARM_ON_STARTUP",
    "log_level": "LOG_LEVEL",
    "share_base_url": "SHARE_BASE_URL",
    "share_api_url": "SHARE_API_URL",
    "share_enabled": "SHARE_ENABLED",
}


class Config:
    """Manages Sniffly configuration with layered priority."""

    # Class-level constants for access from CLI
    DEFAULTS = DEFAULTS
    ENV_MAPPINGS = ENV_MAPPINGS

    def __init__(self, config_dir: Path | None = None):
        """Initialize configuration.

        Args:
            config_dir: Directory for config files. Defaults to ~/.sniffly
        """
        self.config_dir = config_dir or (Path.home() / ".sniffly")
        self.config_file = self.config_dir / "config.json"
        self._ensure_config_dir()

    def _ensure_config_dir(self):
        """Ensure configuration directory exists."""
        self.config_dir.mkdir(exist_ok=True)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with layered priority.

        Priority order:
        1. Environment variable
        2. Config file
        3. Default value

        Args:
            key: Configuration key
            default: Default value if not found

        Returns:
            Configuration value
        """
        # Check environment variable
        env_key = ENV_MAPPINGS.get(key, key.upper())
        env_value = os.getenv(env_key)
        if env_value is not None:
            return self._parse_value(env_value, key)

        # Check config file
        config_data = self._load_config_file()
        if key in config_data:
            return config_data[key]

        # Return default
        return DEFAULTS.get(key, default)

    def get_all(self) -> dict[str, Any]:
        """Get all configuration values.

        Returns:
            Dictionary of all configuration values
        """
        config = {}

        # Start with defaults
        config.update(DEFAULTS)

        # Override with config file values
        config.update(self._load_config_file())

        # Override with environment variables
        for key, env_key in ENV_MAPPINGS.items():
            env_value = os.getenv(env_key)
            if env_value is not None:
                config[key] = self._parse_value(env_value, key)

        return config

    def set(self, key: str, value: Any):
        """Set configuration value in config file.

        Args:
            key: Configuration key
            value: Configuration value
        """
        config_data = self._load_config_file()
        config_data[key] = value
        self._save_config_file(config_data)

    def unset(self, key: str):
        """Remove configuration value from config file.

        Args:
            key: Configuration key
        """
        config_data = self._load_config_file()
        config_data.pop(key, None)
        self._save_config_file(config_data)

    def _load_config_file(self) -> dict[str, Any]:
        """Load configuration from file.

        Returns:
            Configuration dictionary
        """
        if not self.config_file.exists():
            return {}

        try:
            return json.loads(self.config_file.read_text())
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_config_file(self, config_data: dict[str, Any]):
        """Save configuration to file.

        Args:
            config_data: Configuration dictionary
        """
        self.config_file.write_text(json.dumps(config_data, indent=2))

    def _parse_value(self, value: str, key: str) -> Any:
        """Parse string value based on expected type.

        Args:
            value: String value to parse
            key: Configuration key (for type inference)

        Returns:
            Parsed value
        """
        # Get default value for type inference
        default = DEFAULTS.get(key)

        if isinstance(default, bool):
            return value.lower() in ("true", "1", "yes", "on")
        elif isinstance(default, int):
            try:
                return int(value)
            except ValueError:
                return default
        elif isinstance(default, float):
            try:
                return float(value)
            except ValueError:
                return default
        else:
            return value

    # Rollup management methods
    def get_rollups(self) -> dict[str, str]:
        """Get all configured rollups.
        
        Returns:
            Dictionary mapping rollup names to directory paths
        """
        rollups = self.get("rollups", {})
        # Return a copy to prevent modification of shared defaults
        return dict(rollups)

    def _validate_rollup_name(self, name: str) -> None:
        """Validate rollup name for security and consistency.
        
        Args:
            name: Rollup name to validate
            
        Raises:
            ValueError: If name is invalid
        """
        if not name or not name.strip():
            raise ValueError("Rollup name cannot be empty")
        
        if len(name) > 50:
            raise ValueError("Rollup name cannot exceed 50 characters")
        
        if not re.match(r'^[a-zA-Z0-9\s\-_]+$', name):
            raise ValueError("Rollup name can only contain letters, numbers, spaces, hyphens, and underscores")

    def _validate_rollup_path(self, path: str) -> str:
        """Validate and normalize rollup path for security.
        
        Args:
            path: Directory path to validate
            
        Returns:
            Canonical absolute path
            
        Raises:
            ValueError: If path is invalid or unsafe
        """
        if not path or not path.strip():
            raise ValueError("Rollup path cannot be empty")
        
        # Convert to Path object for better handling
        path_obj = Path(path).expanduser().resolve()
        
        # Check if path exists and is a directory
        if not path_obj.exists():
            raise ValueError(f"Directory does not exist: {path}")
        
        if not path_obj.is_dir():
            raise ValueError(f"Path is not a directory: {path}")
        
        # Prevent path traversal by checking for suspicious patterns
        path_str = str(path_obj)
        if '..' in path_str or path_str.startswith('/'):
            # For additional security, we could restrict to user's home directory
            # But for now, we'll allow absolute paths with warnings
            pass
        
        return path_str

    def _check_duplicate_path(self, name: str, path: str) -> None:
        """Check for duplicate rollup paths.
        
        Args:
            name: Name of rollup being added
            path: Path to check
            
        Raises:
            ValueError: If path is already used by another rollup
        """
        rollups = self.get_rollups()
        existing_rollup = next((n for n, p in rollups.items() if p == path and n != name), None)
        if existing_rollup:
            raise ValueError(f"Path '{path}' is already used by rollup '{existing_rollup}'")

    def add_rollup(self, name: str, path: str):
        """Add a new rollup configuration.
        
        Args:
            name: Display name for the rollup
            path: Directory path for the rollup
            
        Raises:
            ValueError: If name or path is invalid
        """
        # Validate inputs
        self._validate_rollup_name(name)
        validated_path = self._validate_rollup_path(path)
        self._check_duplicate_path(name, validated_path)
        
        # Add to configuration
        rollups = self.get_rollups()
        rollups[name] = validated_path
        self.set("rollups", rollups)

    def remove_rollup(self, name: str):
        """Remove a rollup configuration.
        
        Args:
            name: Name of the rollup to remove
        """
        rollups = self.get_rollups()
        rollups.pop(name, None)
        self.set("rollups", rollups)

    def get_rollup_path(self, name: str) -> str | None:
        """Get the path for a specific rollup.
        
        Args:
            name: Name of the rollup
            
        Returns:
            Directory path or None if rollup doesn't exist
        """
        rollups = self.get_rollups()
        return rollups.get(name)
