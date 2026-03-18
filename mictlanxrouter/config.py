"""
Configuration module for the MictlanX Router service.

This file centralizes the reading of environment variables and exposes
typed constants used in server.py and the rest of the project.

The goal is to:
- Have a single point of truth for configuration.
- Provide reasonable default values for development environments.
- Facilitate documentation and maintenance.
"""

import os
from dotenv import load_dotenv
import humanfriendly as HF
from option import Some

# ============================================================
# .env file loading
# ============================================================
# ENV_FILE_PATH allows selecting dynamically which .env file to load
# (for example: .env.local, .env.dev, .env.prod, etc.).
ENV_FILE_PATH = os.environ.get("MICTLANX_ROUTER_ENV_FILE_PATH", ".env.local")
print(f"Loading environment variables from: {ENV_FILE_PATH} - [{'Found' if os.path.exists(ENV_FILE_PATH) else 'Not Found'}]")

if os.path.exists(ENV_FILE_PATH):
    load_dotenv(ENV_FILE_PATH)


# ============================================================
# Internal helpers for casting values
# ============================================================

def _get_bool(name: str, default: str = "0") -> bool:
    """
    Reads an environment variable and converts it to a bool.
    It expects "0" or "1" as a string. Anything else is cast via int().
    """
    return bool(int(os.environ.get(name, default)))


def _get_int(name: str, default: str) -> int:
    """
    Reads an environment variable and converts it to an int.
    """
    return int(os.environ.get(name, default))


def _get_size(name: str, default: str) -> int:
    """
    Reads an environment variable that represents a human-readable size
    (e.g., '1GB', '512MB') and converts it to bytes (int).
    """
    return HF.parse_size(os.environ.get(name, default))


# ==============================
# Debug & Logging
# ==============================
# Flag to enable test mode (affects internal behavior such as using localhost peers, etc.).
MICTLANX_TEST = _get_bool("MICTLANX_TEST", "0")
# Flag to enable debug mode (more logs, console span exporter, etc.).
MICTLANX_ROUTER_DEBUG = _get_bool("MICTLANX_ROUTER_DEBUG", "0")
# Base directory where log files will be stored.
MICTLANX_ROUTER_LOG_PATH = os.environ.get("MICTLANX_ROUTER_LOG_PATH", "/log")
# Base name of the router logger (used in the JSON logger).
MICTLANX_ROUTER_LOG_NAME = os.environ.get("MICTLANX_ROUTER_LOG_NAME", "mictlanx-router-0")
# Log rotation interval (number of units defined by WHEN).
MICTLANX_ROUTER_LOG_INTERVAL = _get_int("MICTLANX_ROUTER_LOG_INTERVAL", "24")
# Time unit for log rotation (e.g., 'h' = hours, 'm' = minutes).
MICTLANX_ROUTER_LOG_WHEN = os.environ.get("MICTLANX_ROUTER_LOG_WHEN", "h")
# Whether to print logs to console (stdout).
MICTLANX_ROUTER_LOG_SHOW = _get_bool("MICTLANX_ROUTER_LOG_SHOW", "1")
# Log level for the router logger (DEBUG, INFO, WARNING, ERROR, etc.).
MICTLANX_ROUTER_LOG_LEVEL = os.environ.get("MICTLANX_ROUTER_LOG_LEVEL","DEBUG",)
# Time unit for log rotation used by the TimedRotatingFileHandler.
# Defaults to the existing MICTLANX_ROUTER_LOG_WHEN, falling back to "h".
MICTLANX_ROUTER_LOG_ROTATION_WHEN = os.environ.get("MICTLANX_ROUTER_LOG_ROTATION_WHEN",MICTLANX_ROUTER_LOG_WHEN,)
# Interval for log rotation (how often the file rotates).
# Defaults to the existing MICTLANX_ROUTER_LOG_INTERVAL.
MICTLANX_ROUTER_LOG_ROTATION_INTERVAL = _get_int("MICTLANX_ROUTER_LOG_ROTATION_INTERVAL",str(MICTLANX_ROUTER_LOG_INTERVAL),)
# Whether to write logs to file (in addition to console).
# 1 = enabled, 0 = disabled.
MICTLANX_ROUTER_LOG_TO_FILE = _get_bool("MICTLANX_ROUTER_LOG_TO_FILE","1",)
# Whether to write ERROR-level logs to a separate error file.
# 1 = enabled, 0 = disabled.
MICTLANX_ROUTER_LOG_ERROR_FILE = _get_bool("MICTLANX_ROUTER_LOG_ERROR_FILE","1",)

# ==============================
# Router Service
# ==============================
# Logical service name for OpenTelemetry / tracer resources.
MICTLANX_ROUTER_SERVICE_NAME = os.environ.get(
    "MICTLANX_ROUTER_SERVICE_NAME",
    "mictlanx-router"
)
# Host where the router HTTP service is exposed.
MICTLANX_ROUTER_HOST = os.environ.get("MICTLANX_ROUTER_HOST", "localhost")
# HTTP port where FastAPI / uvicorn runs.
MICTLANX_ROUTER_PORT = _get_int("MICTLANX_ROUTER_PORT", "60666")
# Maximum number of workers (processes) used by the ASGI server (uvicorn/gunicorn config).
MAX_WORKERS = _get_int("MAX_WORKERS", "4")
MICTLANX_ROUTER_MAX_WORKERS = MAX_WORKERS

# ==============================
# Cache
# ==============================
# Enables or disables the internal cache (CacheFactory vs NoCache).
MICTLANX_CACHE = _get_bool("MICTLANX_CACHE", "1")
# Cache eviction policy (e.g., LRU_SM or other implementations).
MICTLANX_CACHE_EVICTION_POLICY = os.environ.get(
    "MICTLANX_CACHE_EVICTION_POLICY",
    "LRU_SM"
)
# Maximum number of items stored in the cache.
MICTLANX_CACHE_CAPACITY = _get_int("MICTLANX_CACHE_CAPACITY", "100")
# Maximum cache size in bytes (parsed from a human-friendly size).
MICTLANX_CACHE_CAPACITY_STORAGE = _get_size(
    "MICTLANX_CACHE_CAPACITY_STORAGE",
    "1GB"
)

# ==============================
# Peers & Network
# ==============================
# Maximum number of peers the router will manage simultaneously.
MICTLANX_ROUTER_MAX_PEERS = _get_int("MICTLANX_ROUTER_MAX_PEERS", "10")
# Maximum TTL (number of hops) for messages in the network.
MICTLANX_ROUTER_MAX_TTL = _get_int("MICTLANX_ROUTER_MAX_TTL", "3")
# List of logical nodes in the ring (used by some strategies).
MICTLANX_ROUTER_AVAILABLE_NODES = os.environ.get(
    "MICTLANX_ROUTER_AVAILABLE_NODES",
    "0;1;2;3;4;5;6;7;8;9;10"
).split(";")
# Whether to show detailed logs for the replicator.
MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS = _get_bool(
    "MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS",
    "1"
)
# Maximum number of retries for certain operations (e.g., peer connections).
MICTLANX_ROUTER_MAX_TRIES = _get_int("MICTLANX_ROUTER_MAX_TRIES", "60")
# Maximum number of enqueued tasks handled by the router.
MICTLANX_ROUTER_MAX_TASKS = _get_int("MICTLANX_ROUTER_MAX_TASKS", "100")
# Maximum number of concurrent operations (logical concurrency limit).
MICTLANX_ROUTER_MAX_CONCURRENCY = _get_int(
    "MICTLANX_ROUTER_MAX_CONCURRENCY",
    "5"
)
# Maximum number of peers to which data can be replicated (replication factor).
MICTLANX_ROUTER_MAX_PEERS_RF = _get_int("MICTLANX_ROUTER_MAX_PEERS_RF", "5")
# Logical network identifier for MictlanX (useful to isolate multiple networks).
MICTLANX_ROUTER_NETWORK_ID = os.environ.get("MICTLANX_ROUTER_NETWORK_ID", "mictlanx")
# Protocol used by peers (http, https, etc.).
MICTLANX_PROTOCOL = os.environ.get("MICTLANX_PROTOCOL", "http")
# API version exposed by the router.
MICTLANX_API_VERSION = os.environ.get("MICTLANX_API_VERSION", "4")
# URI of known (seed) peers for the router, format like:
# mictlanx://peer-id@host:port,.../?protocol=http&api_version=4
MICTLANX_PEERS_URI = os.environ.get(
    "MICTLANX_PEERS_URI",
    (
        "mictlanx://mictlanx-peer-0@localhost:24000,"
        f"mictlanx-peer-1@localhost:24001/?protocol={MICTLANX_PROTOCOL}"
        f"&api_version={MICTLANX_API_VERSION}"
    ),
)
# Global timeout (in seconds) for long-running / network operations.
MICTLANX_TIMEOUT = _get_int("MICTLANX_TIMEOUT", "3600")
# Maximum size of the internal replicator queue.
MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE = _get_int(
    "MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE",
    "100"
)
# Flag to enable fast synchronization mode in the replicator.
MICTLANX_ROUTER_SYNC_FAST = _get_bool("MICTLANX_ROUTER_SYNC_FAST", "1")

# ==============================
# Summoner Service
# ==============================
# API version of the Summoner service (wrapped in Option[int]).
MICTLANX_SUMMONER_API_VERSION = Some(
    _get_int("MICTLANX_SUMMONER_API_VERSION", "3")
)
# IP address or hostname of the Summoner service.
MICTLANX_SUMMONER_IP_ADDR = os.environ.get(
    "MICTLANX_SUMMONER_IP_ADDR",
    "localhost"
)
# Port where Summoner listens.
MICTLANX_SUMMONER_PORT = _get_int("MICTLANX_SUMMONER_PORT", "15000")
# Summoner operation mode (e.g., 'docker', 'kubernetes', 'local').
MICTLANX_SUMMONER_MODE = os.environ.get("MICTLANX_SUMMONER_MODE", "docker")
# Subnet used by Summoner to create virtual networks (CIDR notation).
MICTLANX_SUMMONER_SUBNET = os.environ.get(
    "MICTLANX_SUMMONER_SUBNET",
    "10.0.0.0/25"
)

# ==============================
# CORS
# ==============================
# Allowed origins for CORS (can be '*' or a comma-separated list).
MICTLANX_CORS_ALLOW_ORIGINS = os.environ.get("MICTLANX_CORS_ALLOW_ORIGINS", "*")
# Whether to allow sending credentials (cookies, auth headers) in CORS.
MICTLANX_CORS_ALLOW_CREDENTIALS = _get_bool(
    "MICTLANX_CORS_ALLOW_CREDENTIALS",
    "1"
)
# Allowed HTTP methods for CORS (e.g., '*', 'GET,POST,PUT').
MICTLANX_CORS_ALLOW_METHODS = os.environ.get("MICTLANX_CORS_ALLOW_METHODS", "*")
# Allowed headers for CORS.
MICTLANX_CORS_ALLOW_HEADERS = os.environ.get("MICTLANX_CORS_ALLOW_HEADERS", "*")

# ==============================
# OpenAPI
# ==============================
# Title shown in the OpenAPI documentation.
MICTLANX_OPEN_API_TITLE = os.environ.get(
    "MICTLANX_OPEN_API_TITLE",
    "MictlanX Router"
)
# API version shown in the documentation (not necessarily equal to MICTLANX_API_VERSION).
MICTLANX_OPEN_API_VERSION = os.environ.get(
    "MICTLANX_OPEN_API_VERSION",
    "0.1.0a0"
)
# Short summary of the service, displayed in the OpenAPI UI.
MICTLANX_OPEN_API_SUMMARY = os.environ.get(
    "MICTLANX_OPEN_API_SUMMARY",
    "MictlanX Router: Virtual storage spaces management."
)
# Optional long description for the OpenAPI documentation.
MICTLANX_OPEN_API_DESCRIPTION = os.environ.get(
    "MICTLANX_OPEN_API_DESCRIPTION",
    ""
)
# URL of a logo to be displayed in OpenAPI info.
MICTLANX_OPEN_API_LOGO = os.environ.get("MICTLANX_OPEN_API_LOGO", "")
# FastAPI root_path, used when mounting the service behind a reverse proxy
# (for example: /mictlanx if the router is published as a subpath).
MICTLANX_OPENAPI_ROOT_PATH = os.environ.get("MICTLANX_ROUTER_OPENAPI_ROOT_PATH", "/mictlanx")

# ==============================
# OpenTelemetry
# ==============================
# OTLP collector endpoint (e.g., Jaeger/Tempo) used to export traces.
MICTLANX_JAEGER_ENDPOINT = os.environ.get(
    "MICTLANX_JAEGER_ENDPOINT",
    "http://localhost:4318"
)
# Flag that indicates whether OpenTelemetry is enabled or a NoOpSpanExporter is used.
MICTLANX_ROUTER_OPENTELEMETRY = _get_bool(
    "MICTLANX_ROUTER_OPENTELEMETRY",
    "1"
)

# ==============================
# Profiler
# ==============================
MICTLANX_ROUTER_PROFILER_OUTPUT_DIR = os.environ.get(
    "MICTLANX_ROUTER_PROFILER_OUTPUT_DIR",
    "./profiles"
)
MICTLANX_ROUTER_PROFILER = _get_bool(
    "MICTLANX_ROUTER_PROFILER",
    "0"
)
MICTLANX_ROUTER_PROFILER_ENABLE_BY_DEFAULT = _get_bool(
    "MICTLANX_ROUTER_PROFILER_ENABLE_BY_DEFAULT",
    "0"
)

MICTLANX_ROUTER_MEMORY_PROFILER_OUTPUT_DIR = os.environ.get(
    "MICTLANX_ROUTER_MEMORY_PROFILER_OUTPUT_DIR",
    "./profiles"
)
MICTLANX_ROUTER_MEMORY_PROFILER = _get_bool(
    "MICTLANX_ROUTER_MEMORY_PROFILER",
    "0"
)
MICTLANX_ROUTER_MEMORY_PROFILER_ENABLE_BY_DEFAULT = _get_bool(
    "MICTLANX_ROUTER_MEMORY_PROFILER_ENABLE_BY_DEFAULT",
    "0"
)
MICTLANX_ROUTER_MEMORY_PROFILER_REPORT_ARGS = os.environ.get(
    "MICTLANX_ROUTER_MEMORY_PROFILER_REPORT_ARGS",
    "--leaks"
)