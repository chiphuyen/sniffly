import asyncio
import json
import logging
import os
import sys
import threading
import time
import webbrowser
from datetime import datetime
from pathlib import Path

import click

from . import __version__
from .config import Config
from .utils.logging import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)


def _setup_event_loop_policy():
    """Set up optimized event loop policy based on platform"""
    try:
        if sys.platform == 'win32':
            # Use winloop on Windows
            import winloop
            asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
            logger.debug("Using winloop event loop policy on Windows")
        else:
            # Use uvloop on other platforms (Linux, macOS)
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.debug("Using uvloop event loop policy")
    except ImportError as e:
        logger.warning(f"Failed to set optimized event loop policy: {e}")
        logger.warning("Falling back to default asyncio event loop policy")


@click.group()
def cli():
    """Sniffly - Claude Code Analytics Tool"""
    pass


@cli.command()
@click.option("--port", type=int, help="Port to run server on")
@click.option("--host", type=str, help="Host to bind server to")
@click.option("--no-browser", is_flag=True, help="Don't open browser automatically")
@click.option("--clear-cache", is_flag=True, help="Clear all caches before starting")
def init(port, host, no_browser, clear_cache):
    """Start the analytics dashboard"""
    # Clear cache if requested
    if clear_cache:
        import shutil
        from pathlib import Path

        # Clear local cache directory
        cache_dir = Path.home() / ".sniffly" / "cache"
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            click.echo(f"✅ Cleared local cache at {cache_dir}")
        else:
            click.echo("ℹ️  No local cache found to clear")

    # Check for first run
    if is_first_run():
        handle_first_run_setup()

    # Get configuration
    cfg = Config()

    # Use provided port or get from config
    if port is None:
        port = cfg.get("port")
    
    # Use provided host or get from config
    if host is None:
        host = cfg.get("host")

    # Determine if we should open browser
    auto_browser = cfg.get("auto_browser")
    should_open_browser = auto_browser and not no_browser

    # Set up optimized event loop for better async performance
    _setup_event_loop_policy()

    # Start server in background thread
    from .server import start_server_with_args

    server_thread = threading.Thread(target=start_server_with_args, args=(port, host), daemon=True)
    server_thread.start()

    # Wait for server to start
    time.sleep(1)

    # Open browser
    if should_open_browser:
        url = f"http://{host}:{port}"
        # Delay browser opening slightly to ensure server is ready
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
        click.echo(f"✨ Sniffly dashboard opened at {url}")
    else:
        click.echo(f"✨ Sniffly running at http://{host}:{port}")

    click.echo("Press Ctrl+C to stop the server")

    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        click.echo("\n👋 Shutting down...")


@cli.command()
def version():
    """Show version information"""
    click.echo(f"sniffly v{__version__}")


@cli.group()
def config():
    """Manage configuration settings"""
    pass


@config.command("show")
@click.option("--json", "as_json", is_flag=True, help="Output in JSON format")
def show_config(as_json):
    """Show current configuration"""
    cfg = Config()
    config_data = cfg.get_all()

    if as_json:
        click.echo(json.dumps(config_data, indent=2))
    else:
        click.echo("Current configuration:")
        for key, value in sorted(config_data.items()):
            # Show source of value
            env_key = Config.ENV_MAPPINGS.get(key, key.upper())
            if os.getenv(env_key) is not None:
                source = " (from environment)"
            elif key in cfg._load_config_file():
                source = " (from config file)"
            else:
                source = " (default)"
            click.echo(f"  {key}: {value}{source}")


@config.command("set")
@click.argument("key")
@click.argument("value")
def set_config(key, value):
    """Set a configuration value"""
    cfg = Config()

    # Validate key
    if key not in Config.DEFAULTS:
        click.echo(f"Error: Unknown configuration key '{key}'")
        click.echo(f"Valid keys: {', '.join(sorted(Config.DEFAULTS.keys()))}")
        return

    # Parse value based on type
    default = Config.DEFAULTS.get(key)
    if isinstance(default, bool):
        value = value.lower() in ("true", "1", "yes", "on")
    elif isinstance(default, int):
        try:
            value = int(value)
        except ValueError:
            click.echo(f"Error: {key} must be an integer")
            return

    cfg.set(key, value)
    click.echo(f"✅ Set {key} = {value}")


@config.command("unset")
@click.argument("key")
def unset_config(key):
    """Remove a configuration value"""
    cfg = Config()
    cfg.unset(key)
    click.echo(f"✅ Removed {key} from config file")


@cli.command("clear-cache")
@click.argument("project", required=False)
def clear_cache(project):
    """Clear memory cache"""
    # For now, this requires server to be running
    # In future, we could implement IPC or file-based cache clearing
    click.echo("Note: Cache clearing requires the server to be running.")
    click.echo("This feature will be implemented in a future version.")
    click.echo("")
    click.echo("For now, restart the server to clear the cache.")


@cli.command(name="help")
def show_help():
    """Show detailed help and usage examples"""
    click.echo(
        """Sniffly - Claude Code Analytics Tool

Usage Examples:

  # Start the dashboard
  sniffly init
  
  # Start on a different port
  sniffly init --port 8090
  
  # Start on a different host
  sniffly init --host 0.0.0.0
  
  # Start without opening browser
  sniffly init --no-browser
  
  # Clear cache and start fresh
  sniffly init --clear-cache
  
  # Show configuration
  sniffly config show
  
  # Set configuration value
  sniffly config set port 8090
  sniffly config set auto_browser false
  
  # Clear cache
  sniffly init --clear-cache
  
  # Show version
  sniffly version

Configuration Keys:
  port                      - Server port (default: 8081)
  host                      - Server host (default: 127.0.0.1)
  cache_max_projects        - Max projects in memory cache (default: 5)
  cache_max_mb_per_project  - Max MB per project (default: 500)
  auto_browser              - Auto-open browser (default: true)
  max_date_range_days       - Max days for date range (default: 30)
  messages_initial_load     - Initial messages to load (default: 500)
  enable_memory_monitor     - Enable memory monitoring (default: false)
  enable_background_processing - Enable background stats (default: true)
  cache_warm_on_startup     - Projects to warm on startup (default: 3)

For more information, visit: https://sniffly.dev
"""
    )


def is_first_run():
    """Check if this is the first time running sniffly"""
    config_path = Path.home() / ".sniffly" / "config.json"
    return not config_path.exists()


def handle_first_run_setup():
    """Handle first-run setup"""
    click.echo("\n🍋 Welcome to Sniffly!")
    click.echo("Your Claude Code analytics dashboard\n")

    # Save config for next time
    config_path = Path.home() / ".sniffly" / "config.json"
    config_path.parent.mkdir(exist_ok=True)
    config_path.write_text(json.dumps({"version": __version__, "first_run": datetime.now().isoformat()}))
