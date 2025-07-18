#!/usr/bin/env python3
"""
FastAPI application for Claude Analytics Dashboard Local Mode
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

# Add parent directory to path for imports (must be before local imports)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import importlib.metadata

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from sniffly.api.messages import get_messages_summary, get_paginated_messages
from sniffly.config import Config
from sniffly.core.processor import ClaudeLogProcessor
from sniffly.utils.cache_warmer import warm_recent_projects
from sniffly.utils.local_cache import LocalCacheService
from sniffly.utils.log_finder import find_claude_logs
from sniffly.utils.memory_cache import MemoryCache

# Load environment variables
load_dotenv()

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO), format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.info(f"Logging configured with level: {log_level}")

# Get version from package metadata
try:
    __version__ = importlib.metadata.version("sniffly")
except importlib.metadata.PackageNotFoundError:
    # Fallback for development mode
    __version__ = "0.1.0"

# Create FastAPI app
app = FastAPI(
    title="Claude Analytics Dashboard - Local Mode",
    description="Analyze your Claude AI logs directly from your local machine",
    version=__version__,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add GZip compression middleware
# Disabled: GZip was actually increasing load time for large payloads
# app.add_middleware(GZipMiddleware, minimum_size=1000)

# Initialize configuration
config = Config()

# Initialize services
cache_service = LocalCacheService()

# Get memory cache configuration using Config
max_projects = config.get("cache_max_projects")
max_mb_per_project = config.get("cache_max_mb_per_project")
cache_warm_on_startup = config.get("cache_warm_on_startup")
enable_background_processing = config.get("enable_background_processing")

memory_cache = MemoryCache(max_projects=max_projects, max_mb_per_project=max_mb_per_project)
logger.info(f"Memory cache initialized: {max_projects} projects, {max_mb_per_project}MB per project")

# Get base directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Mount static files
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

# Current project being analyzed
current_project_path: str | None = None
current_log_path: str | None = None


async def background_stats_processor():
    """Background task to process stats for all projects."""
    logger.debug("[Server] Starting background stats processor")

    # Wait a bit for server to fully start
    await asyncio.sleep(5)

    while True:
        try:
            # Get all projects
            from sniffly.utils.log_finder import get_all_projects_with_metadata

            all_projects = get_all_projects_with_metadata()

            # Find projects without cached stats
            uncached_projects = []
            for project in all_projects:
                log_path = project["log_path"]
                # Skip if in memory cache
                if memory_cache.get(log_path):
                    continue
                # Skip if in file cache
                if cache_service.get_cached_stats(log_path):
                    continue
                uncached_projects.append(project)

            if uncached_projects:
                logger.debug(f"[Background] Found {len(uncached_projects)} projects without cached stats")

                # Process them using global aggregator
                from sniffly.core.global_aggregator import GlobalStatsAggregator

                aggregator = GlobalStatsAggregator(memory_cache, cache_service)
                processed = await aggregator.process_uncached_projects(uncached_projects, limit=5)

                logger.debug(f"[Background] Processed {processed} projects")

                # Wait before next batch
                await asyncio.sleep(30)
            else:
                logger.debug("[Background] All projects have cached stats")
                # Check again in 5 minutes
                await asyncio.sleep(300)

        except Exception as e:
            logger.error(f"[Background] Error in stats processor: {e}")
            await asyncio.sleep(60)


# Startup event - warm cache on server start
@app.on_event("startup")
async def startup_event():
    """Start background tasks on startup"""

    # Run cache warming in the background so server can start accepting requests immediately
    async def warm_cache_background():
        logger.debug(f"[Server] Starting cache warming in background ({cache_warm_on_startup} projects)")
        await warm_recent_projects(
            cache_service, memory_cache, current_log_path, exclude_current=False, limit=cache_warm_on_startup
        )
        logger.debug("[Server] Cache warming completed")

    # Start cache warming in background
    asyncio.create_task(warm_cache_background())

    # Start background task to process remaining projects
    if enable_background_processing:
        logger.debug("[Server] Background processing enabled - starting stats processor")
        asyncio.create_task(background_stats_processor())
    else:
        logger.debug("[Server] Background processing disabled")


# Root endpoint - serve overview page
@app.get("/")
async def root():
    """Serve the global overview page"""
    overview_path = os.path.join(BASE_DIR, "templates", "overview.html")
    # Fallback to dashboard if overview doesn't exist yet
    if not os.path.exists(overview_path):
        return FileResponse(os.path.join(BASE_DIR, "templates", "dashboard.html"))
    return FileResponse(overview_path)


# Dashboard page
@app.get("/dashboard.html")
async def dashboard_page():
    """Serve the dashboard page"""
    return FileResponse(os.path.join(BASE_DIR, "templates", "dashboard.html"))


# Project-specific dashboard URLs
@app.get("/project/{project_name:path}")
async def project_dashboard(project_name: str):
    """Serve the dashboard for a specific project"""
    # The project_name is the directory name from .claude/projects
    # The dashboard.html will use JavaScript to extract this from the URL
    return FileResponse(os.path.join(BASE_DIR, "templates", "dashboard.html"))


# Set project endpoint
@app.post("/api/project")
async def set_project(data: dict[str, str]):
    """Set the project path to analyze"""
    global current_project_path, current_log_path

    project_path = data.get("project_path")
    if not project_path:
        raise HTTPException(status_code=400, detail="Project path is required")

    # Validate project path exists
    if not os.path.exists(project_path):
        raise HTTPException(status_code=400, detail=f"Project path does not exist: {project_path}")

    # Find Claude logs for this project
    log_path = find_claude_logs(project_path)
    if not log_path or not os.path.exists(log_path):
        raise HTTPException(
            status_code=404,
            detail=f"Claude logs not found for project: {project_path}. "
            f"Make sure you have used Claude with this project.",
        )

    current_project_path = project_path
    current_log_path = log_path

    return JSONResponse(
        {
            "status": "success",
            "project_path": project_path,
            "log_path": log_path,
            "message": "Project set successfully. You can now view the dashboard.",
        }
    )


# Get current project
@app.get("/api/project")
async def get_current_project():
    """Get the current project being analyzed"""
    if not current_project_path:
        return JSONResponse({"status": "no_project", "message": "No project selected"})

    return JSONResponse(
        {
            "status": "active",
            "project_path": current_project_path,
            "log_path": current_log_path,
            "log_dir_name": Path(current_log_path).name if current_log_path else None,
        }
    )


# Set project by log directory name
@app.post("/api/project-by-dir")
async def set_project_by_dir(data: dict[str, str]):
    """Set the project by log directory name"""
    global current_project_path, current_log_path

    dir_name = data.get("dir_name")
    if not dir_name:
        raise HTTPException(status_code=400, detail="Directory name is required")

    # Build the log path
    claude_base = Path.home() / ".claude" / "projects"
    log_path = claude_base / dir_name

    if not log_path.exists() or not log_path.is_dir():
        raise HTTPException(status_code=404, detail=f"Log directory not found: {dir_name}")

    # Check if it has log files
    log_files = list(log_path.glob("*.jsonl"))
    if not log_files:
        raise HTTPException(status_code=404, detail=f"No log files found in directory: {dir_name}")

    # Try to convert back to project path (best effort)
    if dir_name.startswith("-"):
        # Convert from hashed format
        project_path = dir_name[1:].replace("-", "/")
    else:
        # Use directory name as is
        project_path = dir_name

    current_project_path = project_path
    current_log_path = str(log_path)

    # Pre-warm the current project immediately
    if not memory_cache.get(current_log_path):
        logger.debug(f"Pre-warming {dir_name}...")
        try:
            processor = ClaudeLogProcessor(current_log_path)
            messages, stats = processor.process_logs()

            # Save to file cache first (creates metadata)
            cache_service.save_cached_stats(current_log_path, stats)
            cache_service.save_cached_messages(current_log_path, messages)

            # Then store in memory cache
            memory_cache.put(current_log_path, messages, stats)
            logger.debug(f"Successfully pre-warmed {dir_name}")
        except Exception as e:
            logger.debug(f"Failed to pre-warm: {e}")

    # Warm cache for other recent projects in background
    asyncio.create_task(warm_recent_projects(cache_service, memory_cache, current_log_path, exclude_current=True))

    return JSONResponse(
        {
            "status": "success",
            "project_path": project_path,
            "log_path": str(log_path),
            "log_dir_name": dir_name,
            "message": f"Now analyzing logs from: {dir_name}",
        }
    )


# Stats endpoint - process or get cached
@app.get("/api/stats")
async def get_stats(timezone_offset: int = 0):
    """Get statistics for the current project"""
    import time

    start_time = time.time()

    if not current_log_path:
        raise HTTPException(status_code=400, detail="No project selected")

    # Check memory cache first (L1)
    memory_result = memory_cache.get(current_log_path)
    if memory_result:
        messages, stats = memory_result
        elapsed_ms = (time.time() - start_time) * 1000
        logger.debug(f"Memory cache hit - {elapsed_ms:.2f}ms")

        # Ensure file cache is populated for persistence across restarts
        if not cache_service.get_cached_stats(current_log_path):
            cache_service.save_cached_stats(current_log_path, stats)
            cache_service.save_cached_messages(current_log_path, messages)
            logger.debug("Persisted to file cache")

        return stats

    # Check file cache next (L2)
    cached_stats = cache_service.get_cached_stats(current_log_path)
    if cached_stats and not cache_service.has_changes(current_log_path):
        # Also get messages to store in memory cache
        cached_messages = cache_service.get_cached_messages(current_log_path)
        if cached_messages:
            # Promote to memory cache
            memory_cache.put(current_log_path, cached_messages, cached_stats)
            elapsed_ms = (time.time() - start_time) * 1000
            logger.debug(f"File cache hit - {elapsed_ms:.2f}ms")
        return cached_stats

    # Process logs
    try:
        process_start = time.time()
        processor = ClaudeLogProcessor(current_log_path)
        messages, statistics = processor.process_logs(timezone_offset_minutes=timezone_offset)
        process_time = (time.time() - process_start) * 1000

        # Cache the results
        cache_service.save_cached_stats(current_log_path, statistics)
        cache_service.save_cached_messages(current_log_path, messages)

        # Also store in memory cache
        memory_cache.put(current_log_path, messages, statistics)

        total_time = (time.time() - start_time) * 1000
        logger.debug(f"Cache miss - Total: {total_time:.2f}ms (Processing: {process_time:.2f}ms)")

        return statistics

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing logs: {str(e)}")


# Optimized dashboard data endpoint
@app.get("/api/dashboard-data")
async def get_dashboard_data(timezone_offset: int = 0):
    """Get optimized data for initial dashboard load"""
    import time

    start_time = time.time()

    if not current_log_path:
        raise HTTPException(status_code=400, detail="No project selected")

    # Get data from cache or process
    memory_result = memory_cache.get(current_log_path)
    if memory_result:
        messages, stats = memory_result
        elapsed_ms = (time.time() - start_time) * 1000
        logger.debug(f"Memory cache hit - {elapsed_ms:.2f}ms")

        # Adjust timezone-sensitive statistics if needed
        if timezone_offset != 0:
            from sniffly.core.stats import StatisticsGenerator

            tz_start = time.time()
            generator = StatisticsGenerator(current_log_path, {})
            stats["daily_stats"] = generator._calculate_daily_stats(messages, timezone_offset)
            stats["hourly_pattern"] = generator._calculate_hourly_pattern(messages, timezone_offset)
            tz_time = (time.time() - tz_start) * 1000
            logger.debug(f"Timezone adjustment took {tz_time:.2f}ms")
    else:
        # Try file cache or process
        cached_stats = cache_service.get_cached_stats(current_log_path)
        cached_messages = cache_service.get_cached_messages(current_log_path)

        if cached_stats and cached_messages and not cache_service.has_changes(current_log_path):
            # File cache hit
            messages, stats = cached_messages, cached_stats
            file_cache_time = (time.time() - start_time) * 1000
            logger.debug(f"File cache hit - {file_cache_time:.2f}ms")

            # Adjust timezone-sensitive statistics if needed
            if timezone_offset != 0:
                from sniffly.core.stats import StatisticsGenerator

                tz_start = time.time()
                generator = StatisticsGenerator(current_log_path, {})
                stats["daily_stats"] = generator._calculate_daily_stats(messages, timezone_offset)
                stats["hourly_pattern"] = generator._calculate_hourly_pattern(messages, timezone_offset)
                tz_time = (time.time() - tz_start) * 1000

            # Promote to memory cache
            memory_cache.put(current_log_path, messages, stats)
        else:
            # Process from scratch
            process_start = time.time()
            processor = ClaudeLogProcessor(current_log_path)
            messages, stats = processor.process_logs(timezone_offset_minutes=timezone_offset)
            process_time = (time.time() - process_start) * 1000
            logger.debug(f"Processing took {process_time:.2f}ms")

            # Save to caches
            cache_start = time.time()
            cache_service.save_cached_stats(current_log_path, stats)
            cache_service.save_cached_messages(current_log_path, messages)
            memory_cache.put(current_log_path, messages, stats)
            cache_time = (time.time() - cache_start) * 1000
            logger.debug(f"Cache storage took {cache_time:.2f}ms")

    # Return optimized payload (no chart_messages needed anymore)
    transform_start = time.time()
    first_page = get_paginated_messages(messages, page=1, per_page=50)
    transform_time = (time.time() - transform_start) * 1000
    logger.debug(f"Data transformation took {transform_time:.2f}ms")

    response_time = (time.time() - start_time) * 1000
    logger.debug(f"Total response time: {response_time:.2f}ms")

    return {
        "statistics": stats,
        "messages_page": first_page,
        "message_count": len(messages),
        "config": {
            "messages_initial_load": config.get("messages_initial_load"),
            "enable_memory_monitor": config.get("enable_memory_monitor"),
            "max_date_range_days": config.get("max_date_range_days"),
        },
    }


# Messages endpoint - process or get cached
@app.get("/api/messages")
async def get_messages(limit: int | None = None, timezone_offset: int = 0):
    """Get messages for the current project"""
    import time

    start_time = time.time()

    if not current_log_path:
        raise HTTPException(status_code=400, detail="No project selected")

    # Check memory cache first (L1)
    memory_result = memory_cache.get(current_log_path)
    if memory_result:
        messages, stats = memory_result
        elapsed_ms = (time.time() - start_time) * 1000
        logger.debug(f"Memory cache hit - {elapsed_ms:.2f}ms")

        # Apply limit if requested
        if limit and limit < len(messages):
            return messages[:limit]
        return messages

    # Check file cache next (L2)
    cached_messages = cache_service.get_cached_messages(current_log_path)
    if cached_messages and not cache_service.has_changes(current_log_path):
        # Also get stats to store in memory cache
        cached_stats = cache_service.get_cached_stats(current_log_path)
        if cached_stats:
            # Promote to memory cache
            memory_cache.put(current_log_path, cached_messages, cached_stats)
            elapsed_ms = (time.time() - start_time) * 1000
            logger.debug(f"File cache hit - {elapsed_ms:.2f}ms")
        # Apply limit if requested
        if limit and limit < len(cached_messages):
            return cached_messages[:limit]
        return cached_messages

    # Process logs
    try:
        process_start = time.time()
        processor = ClaudeLogProcessor(current_log_path)
        messages, statistics = processor.process_logs(timezone_offset_minutes=timezone_offset)
        process_time = (time.time() - process_start) * 1000

        # Cache the results
        cache_service.save_cached_stats(current_log_path, statistics)
        cache_service.save_cached_messages(current_log_path, messages)

        # Also store in memory cache
        memory_cache.put(current_log_path, messages, statistics)

        total_time = (time.time() - start_time) * 1000
        logger.debug(f"Cache miss - Total: {total_time:.2f}ms (Processing: {process_time:.2f}ms)")

        # Apply limit if requested
        if limit and limit < len(messages):
            return messages[:limit]
        return messages

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing logs: {str(e)}")


# Messages summary endpoint - lightweight stats only
@app.get("/api/messages/summary")
async def get_messages_summary_endpoint():
    """Get summary statistics about messages without loading all data"""
    if not current_log_path:
        raise HTTPException(status_code=400, detail="No project selected")

    # Get from memory cache if available
    memory_result = memory_cache.get(current_log_path)
    if memory_result:
        messages, _ = memory_result
        return get_messages_summary(messages)

    # Otherwise need to load messages
    cached_messages = cache_service.get_cached_messages(current_log_path)
    if cached_messages:
        return get_messages_summary(cached_messages)

    # Process if needed
    processor = ClaudeLogProcessor(current_log_path)
    messages, _ = processor.process_logs()
    return get_messages_summary(messages)


# Refresh endpoint - smart refresh only if changed
@app.post("/api/refresh")
async def refresh_data(request: dict):
    """Refresh project data only if files have changed.

    This endpoint implements smart refresh functionality:
    1. Checks if log files have changed using file metadata (size + mtime)
    2. If no changes: returns immediately, keeping caches intact (<5ms)
    3. If changes detected: invalidates caches and reprocesses data

    Args:
        request: JSON body with optional 'timezone_offset' in minutes

    Returns:
        JSON response with:
        - status: "success"
        - message: Description of what happened
        - files_changed: Boolean indicating if files changed
        - refresh_time_ms: Time taken in milliseconds
        - message_count: Number of messages (only if files changed)
    """
    # If no current project selected, this is likely from the overview page
    # In that case, check all projects for changes
    if not current_log_path:
        return await refresh_all_projects(request)

    # Extract timezone offset from request body
    timezone_offset = request.get("timezone_offset", 0)

    import time

    start_time = time.time()

    # Check if files have changed
    has_changes = cache_service.has_changes(current_log_path)

    if not has_changes:
        # No changes - keep memory cache intact for fast reload
        refresh_time = (time.time() - start_time) * 1000
        logger.debug(f"No changes detected - keeping cache in {refresh_time:.2f}ms")

        return JSONResponse(
            {
                "status": "success",
                "message": "No changes detected - using cached data",
                "files_changed": False,
                "refresh_time_ms": refresh_time,
            }
        )

    # Files have changed - reprocess
    try:
        # Invalidate caches
        memory_cache.invalidate(current_log_path)
        cache_service.invalidate_cache(current_log_path)

        # Reprocess
        processor = ClaudeLogProcessor(current_log_path)
        messages, statistics = processor.process_logs(timezone_offset_minutes=timezone_offset)

        # Cache the new results
        cache_service.save_cached_stats(current_log_path, statistics)
        cache_service.save_cached_messages(current_log_path, messages)
        memory_cache.put(current_log_path, messages, statistics)

        refresh_time = (time.time() - start_time) * 1000
        logger.debug(f"Files changed - data refreshed in {refresh_time:.2f}ms")

        return JSONResponse(
            {
                "status": "success",
                "message": "Files changed - data refreshed successfully",
                "files_changed": True,
                "message_count": len(messages),
                "refresh_time_ms": refresh_time,
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error refreshing data: {str(e)}")


async def refresh_all_projects(request: dict):
    """Refresh all projects from the overview page.

    This checks each project for changes and updates only those that have changed.
    """
    import time

    start_time = time.time()

    projects_refreshed = 0
    total_projects = 0

    # Get all Claude log directories
    from .utils.log_finder import get_all_projects_with_metadata

    all_projects = get_all_projects_with_metadata()
    total_projects = len(all_projects)

    for project in all_projects:
        log_path = project["log_path"]

        # Check if this project has changed using the has_changes method
        has_changed = cache_service.has_changes(log_path)

        if has_changed:
            # Invalidate caches for this project
            cache_service.invalidate_cache(log_path)
            memory_cache.invalidate(log_path)

            # Process the project to update caches
            try:
                processor = ClaudeLogProcessor(log_path)
                messages, stats = processor.process_logs()

                # Cache the results
                cache_service.save_cached_stats(log_path, stats)
                cache_service.save_cached_messages(log_path, messages)
                memory_cache.put(log_path, messages, stats)

                projects_refreshed += 1
                logger.debug(f"Refreshed project: {project['display_name']}")
            except Exception as e:
                logger.error(f"Error refreshing project {project['display_name']}: {e}")

    elapsed_ms = int((time.time() - start_time) * 1000)

    if projects_refreshed > 0:
        message = f"Refreshed {projects_refreshed} of {total_projects} projects"
    else:
        message = "No changes detected in any project"

    return JSONResponse(
        {
            "status": "success",
            "message": message,
            "files_changed": projects_refreshed > 0,
            "refresh_time_ms": elapsed_ms,
            "projects_refreshed": projects_refreshed,
            "total_projects": total_projects,
        }
    )


# Cache status endpoint
@app.get("/api/cache/status")
async def get_cache_status():
    """Get cache statistics"""
    memory_stats = memory_cache.get_stats()

    # Add information about current project
    current_project_info = None
    if current_log_path:
        current_project_info = memory_cache.get_project_info(current_log_path)

    return JSONResponse(
        {"memory_cache": memory_stats, "current_project": current_project_info, "current_log_path": current_log_path}
    )


# Get JSONL files for current project
@app.get("/api/jsonl-files")
async def get_jsonl_files(project: str | None = None):
    """Get list of JSONL files for a project with metadata"""
    log_path = current_log_path

    # If specific project provided, use that
    if project:
        claude_base = Path.home() / ".claude" / "projects"
        log_path = str(claude_base / project)

    if not log_path:
        raise HTTPException(status_code=400, detail="No project selected")

    try:
        log_dir = Path(log_path)
        if not log_dir.exists():
            raise HTTPException(status_code=404, detail="Log directory not found")

        # Get all JSONL files with metadata
        files_with_metadata = []
        for f in log_dir.glob("*.jsonl"):
            stat = f.stat()

            # Try to get first and last timestamps from file content
            first_timestamp = None
            last_timestamp = None
            try:
                with open(f) as file:
                    # Read first line
                    first_line = file.readline()
                    if first_line:
                        import json

                        first_data = json.loads(first_line)
                        first_timestamp = first_data.get("timestamp")

                    # Read last line efficiently
                    # Seek to end and read backwards to find last complete line
                    file.seek(0, 2)  # Go to end of file
                    file_length = file.tell()

                    # Read last 4KB (should be enough for last line)
                    seek_pos = max(0, file_length - 4096)
                    file.seek(seek_pos)
                    last_chunk = file.read()

                    # Find last complete line
                    lines = last_chunk.strip().split("\n")
                    if lines:
                        last_line = lines[-1]
                        try:
                            last_data = json.loads(last_line)
                            last_timestamp = last_data.get("timestamp")
                        except (json.JSONDecodeError, ValueError):
                            # If last line is incomplete, try second to last
                            if len(lines) > 1:
                                try:
                                    last_data = json.loads(lines[-2])
                                    last_timestamp = last_data.get("timestamp")
                                except (json.JSONDecodeError, ValueError):
                                    pass
            except Exception:
                pass

            # Convert timestamps to unix time if available
            created_time = stat.st_ctime
            modified_time = stat.st_mtime

            if first_timestamp:
                try:
                    from datetime import datetime

                    dt = datetime.fromisoformat(first_timestamp.replace("Z", "+00:00"))
                    created_time = dt.timestamp()
                except (ValueError, AttributeError):
                    pass

            if last_timestamp:
                try:
                    from datetime import datetime

                    dt = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
                    modified_time = dt.timestamp()
                except (ValueError, AttributeError):
                    pass

            files_with_metadata.append(
                {"name": f.name, "created": created_time, "modified": modified_time, "size": stat.st_size}
            )

        # Sort by creation time (earliest first)
        files_with_metadata.sort(key=lambda x: x["created"])

        return JSONResponse(files_with_metadata)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading log files: {str(e)}")


# Get JSONL file content
@app.get("/api/jsonl-content")
async def get_jsonl_content(file: str, project: str | None = None):
    """Get content of a specific JSONL file"""
    log_path = current_log_path

    # If specific project provided, use that
    if project:
        claude_base = Path.home() / ".claude" / "projects"
        log_path = str(claude_base / project)

    if not log_path:
        raise HTTPException(status_code=400, detail="No project selected")

    try:
        file_path = Path(log_path) / file
        if not file_path.exists() or not file_path.suffix == ".jsonl":
            raise HTTPException(status_code=404, detail="File not found")

        # Get file metadata
        stat = file_path.stat()
        file_size = stat.st_size
        created_time = stat.st_ctime
        modified_time = stat.st_mtime

        lines = []
        user_count = 0
        assistant_count = 0
        first_timestamp = None
        last_timestamp = None
        session_id = None
        cwd = None

        with open(file_path) as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line)
                    lines.append(data)

                    # Extract metadata
                    if line_num == 1:
                        # Use filename as session ID since files can contain multiple sessions
                        session_id = file_path.stem
                        cwd = data.get("cwd", "")

                    # Track timestamps
                    if data.get("timestamp"):
                        if not first_timestamp:
                            first_timestamp = data["timestamp"]
                        last_timestamp = data["timestamp"]

                    # Count types
                    if data.get("type") == "user":
                        user_count += 1
                    elif data.get("type") == "assistant":
                        assistant_count += 1
                except json.JSONDecodeError:
                    # Include malformed lines for debugging
                    lines.append({"error": "JSON decode error", "line_number": line_num, "raw": line[:200]})

        # Calculate duration if we have timestamps
        duration_minutes = None
        if first_timestamp and last_timestamp:
            try:
                from datetime import datetime

                start = datetime.fromisoformat(first_timestamp.replace("Z", "+00:00"))
                end = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
                duration_minutes = (end - start).total_seconds() / 60

                # Use actual timestamps from content instead of file metadata
                created_time = start.timestamp()
                modified_time = end.timestamp()
            except (ValueError, AttributeError, KeyError):
                pass

        return JSONResponse(
            {
                "lines": lines,
                "total_lines": len(lines),
                "user_count": user_count,
                "assistant_count": assistant_count,
                "metadata": {
                    "session_id": session_id,
                    "file_size": file_size,
                    "created": created_time,
                    "modified": modified_time,
                    "first_timestamp": first_timestamp,
                    "last_timestamp": last_timestamp,
                    "duration_minutes": duration_minutes,
                    "cwd": cwd,
                },
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")


# Get recent projects from Claude logs directory
@app.get("/api/recent-projects")
async def get_recent_projects():
    """Get list of recent projects from Claude logs"""
    try:
        claude_base = Path.home() / ".claude" / "projects"
        if not claude_base.exists():
            return JSONResponse({"projects": []})

        # Get all project directories
        projects = []
        for project_dir in claude_base.iterdir():
            if project_dir.is_dir():
                # Check if it has log files
                log_files = list(project_dir.glob("*.jsonl"))
                if log_files:
                    # Get most recent modification time
                    latest_mod = max(f.stat().st_mtime for f in log_files)
                    projects.append(
                        {
                            "dir_name": project_dir.name,  # The actual directory name
                            "log_path": str(project_dir),
                            "last_modified": latest_mod,
                            "file_count": len(log_files),
                        }
                    )

        # Sort by last modified, most recent first
        projects.sort(key=lambda x: x["last_modified"], reverse=True)

        # Return top 20 to show more options
        return JSONResponse({"projects": projects[:20]})

    except Exception as e:
        return JSONResponse({"projects": [], "error": str(e)})


# Comprehensive projects endpoint for global stats
@app.get("/api/projects")
async def get_projects(
    include_stats: bool = False, sort_by: str = "last_modified", limit: int | None = None, offset: int = 0
):
    """
    Get all available Claude projects with metadata.

    Args:
        include_stats: Include statistics for each project (may be slower)
        sort_by: Sort field (last_modified, first_seen, size, name)
        limit: Maximum number of projects to return
        offset: Offset for pagination

    Returns:
        JSON with projects list and metadata
    """
    from sniffly.utils.log_finder import get_all_projects_with_metadata

    try:
        # Get all projects with metadata
        projects = get_all_projects_with_metadata()

        # Add cache status and URL slug for each project
        for project in projects:
            project["in_cache"] = memory_cache.get(project["log_path"]) is not None
            project["url_slug"] = project["dir_name"]  # Use dir name for URLs

        if include_stats:
            # Add statistics from cache for cached projects
            for project in projects:
                if project["in_cache"]:
                    cache_result = memory_cache.get(project["log_path"])
                    if cache_result:
                        _, stats = cache_result
                        # Extract stats from nested structure
                        overview = stats.get("overview", {})
                        total_tokens = overview.get("total_tokens", {})
                        user_interactions = stats.get("user_interactions", {})

                        project["stats"] = {
                            "total_input_tokens": total_tokens.get("input", 0),
                            "total_output_tokens": total_tokens.get("output", 0),
                            "total_cache_read": total_tokens.get("cache_read", 0),
                            "total_cache_write": total_tokens.get("cache_creation", 0),
                            "total_commands": user_interactions.get("user_commands_analyzed", 0),
                            "avg_tokens_per_command": user_interactions.get("avg_tokens_per_command", 0),
                            "avg_steps_per_command": user_interactions.get("avg_steps_per_command", 0),
                            "compact_summary_count": overview.get("message_types", {}).get("compact_summary", 0),
                            "first_message_date": overview.get("date_range", {}).get("start"),
                            "last_message_date": overview.get("date_range", {}).get("end"),
                            "total_cost": overview.get("total_cost", 0),
                        }
                        logger.debug(f"Added stats for cached project {project['dir_name']}: {project['stats']}")
                    else:
                        # Cache was evicted between status check and retrieval
                        project["in_cache"] = False
                else:
                    # Try file cache
                    cached_stats = cache_service.get_cached_stats(project["log_path"])
                    if cached_stats:
                        # Extract stats from nested structure (same as memory cache)
                        overview = cached_stats.get("overview", {})
                        total_tokens = overview.get("total_tokens", {})
                        user_interactions = cached_stats.get("user_interactions", {})

                        project["stats"] = {
                            "total_input_tokens": total_tokens.get("input", 0),
                            "total_output_tokens": total_tokens.get("output", 0),
                            "total_cache_read": total_tokens.get("cache_read", 0),
                            "total_cache_write": total_tokens.get("cache_creation", 0),
                            "total_commands": user_interactions.get("user_commands_analyzed", 0),
                            "avg_tokens_per_command": user_interactions.get("avg_tokens_per_command", 0),
                            "avg_steps_per_command": user_interactions.get("avg_steps_per_command", 0),
                            "compact_summary_count": overview.get("message_types", {}).get("compact_summary", 0),
                            "first_message_date": overview.get("date_range", {}).get("start"),
                            "last_message_date": overview.get("date_range", {}).get("end"),
                            "total_cost": overview.get("total_cost", 0),
                        }
                    else:
                        project["stats"] = None  # Will need to load in background

        # Sort projects
        if sort_by == "last_modified":
            projects.sort(key=lambda x: x["last_modified"], reverse=True)
        elif sort_by == "first_seen":
            projects.sort(key=lambda x: x["first_seen"])
        elif sort_by == "size":
            projects.sort(key=lambda x: x["total_size_mb"], reverse=True)
        elif sort_by == "name":
            projects.sort(key=lambda x: x["display_name"])

        # Apply pagination
        total_count = len(projects)
        if limit:
            projects = projects[offset : offset + limit]

        return JSONResponse(
            {
                "projects": projects,
                "total_count": total_count,
                "has_more": offset + limit < total_count if limit else False,
                "cache_status": {
                    "cached_count": sum(1 for p in projects if p["in_cache"]),
                    "total_projects": total_count,
                },
            }
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return JSONResponse({"error": f"Failed to get projects: {str(e)}"}, status_code=500)


# Global statistics endpoint
@app.get("/api/global-stats")
async def get_global_stats():
    """
    Get aggregated statistics across all projects.

    Returns:
        JSON with global statistics including charts data
    """
    from sniffly.core.global_aggregator import GlobalStatsAggregator
    from sniffly.utils.log_finder import get_all_projects_with_metadata

    try:
        # Get all projects
        projects = get_all_projects_with_metadata()

        # Add cache status
        for project in projects:
            project["in_cache"] = memory_cache.get(project["log_path"]) is not None

        # Create aggregator and get global stats
        aggregator = GlobalStatsAggregator(memory_cache, cache_service)
        global_stats = await aggregator.get_global_stats(projects)

        # Add configuration
        global_stats["config"] = {"max_date_range_days": config.get("max_date_range_days")}

        return JSONResponse(global_stats)

    except Exception as e:
        import traceback

        traceback.print_exc()
        return JSONResponse({"error": f"Failed to get global stats: {str(e)}"}, status_code=500)


# Pricing endpoints
@app.get("/api/pricing")
async def get_pricing():
    """Get current model pricing"""
    try:
        from sniffly.services.pricing_service import PricingService

        service = PricingService()
        pricing_data = service.get_pricing()

        return JSONResponse(
            {
                "pricing": pricing_data["pricing"],
                "source": pricing_data["source"],
                "timestamp": pricing_data["timestamp"],
                "is_stale": pricing_data.get("is_stale", False),
            }
        )
    except Exception as e:
        return JSONResponse({"error": f"Failed to get pricing: {str(e)}"}, status_code=500)


@app.post("/api/pricing/refresh")
async def refresh_pricing():
    """Force refresh pricing data"""
    try:
        from sniffly.services.pricing_service import PricingService

        service = PricingService()
        success = service.force_refresh()

        if success:
            return JSONResponse({"status": "success", "message": "Pricing updated successfully"})
        else:
            return JSONResponse({"status": "error", "message": "Failed to fetch pricing from LiteLLM"}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": f"Failed to refresh pricing: {str(e)}"}, status_code=500)


# Share endpoints
@app.get("/api/share-enabled")
async def share_enabled():
    """Check if sharing is enabled"""
    return {"enabled": True}


@app.post("/api/share")
async def create_share_link(data: dict[str, Any], request: Request):
    """Create a shareable link for the dashboard"""
    try:
        from sniffly.share import ShareManager

        share_manager = ShareManager()

        # Extract request info for logging
        request_info = {
            "ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
        }

        # Create share link
        result = await share_manager.create_share_link(
            statistics=data.get("statistics", {}),
            charts_data=data.get("charts", {}),
            make_public=data.get("make_public", False),
            include_commands=data.get("include_commands", False),
            user_commands=data.get("user_commands", []),
            project_name=data.get("project_name"),
            request_info=request_info,
        )

        return result
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# Health check
@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "mode": "local"}


# Favicon endpoint to prevent 404 errors
@app.get("/favicon.ico")
async def favicon():
    """Return empty favicon to prevent 404 errors"""
    return (
        FileResponse(os.path.join(BASE_DIR, "static", "favicon.ico"), media_type="image/x-icon")
        if os.path.exists(os.path.join(BASE_DIR, "static", "favicon.ico"))
        else JSONResponse(content={}, status_code=204)
    )


def start_server_with_args(port=8081, host="localhost"):
    """Start the server with specified arguments"""
    import uvicorn

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8081)
