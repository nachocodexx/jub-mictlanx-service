import os 
import sys
from mictlanxrouter import config
IS_TEST_ENV = os.environ.get("GITHUB_ACTIONS") == "true" or "pytest"in sys.modules
print("IS_TEST_ENV:", IS_TEST_ENV)

if not IS_TEST_ENV:
    # Open telemetry
    #_______________________
    from mictlanxrouter.opentelemetry import NoOpSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter
    )
    from prometheus_fastapi_instrumentator import Instrumentator
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    resource = Resource(
        attributes={
            SERVICE_NAME: config.MICTLANX_ROUTER_SERVICE_NAME
        }
    )


    trace_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(trace_provider)
    tracer = trace.get_tracer(config.MICTLANX_ROUTER_SERVICE_NAME)


    if not config.MICTLANX_ROUTER_OPENTELEMETRY:
        trace_provider.add_span_processor(SimpleSpanProcessor(NoOpSpanExporter()))
    else:
        # ===========================================================
        # JAEGER
        # ===========================================================
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="{}/v1/traces".format(config.MICTLANX_JAEGER_ENDPOINT)))
        trace_provider.add_span_processor(processor)

    # ===========================================================
    # CONSOLE_SPAN_EXPORTER
    # ===========================================================
    if config.MICTLANX_ROUTER_DEBUG:
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        trace_provider.add_span_processor(processor)
else:
    # Mocking OpenTelemetry components for testing environment like Github Actions
    from unittest.mock import MagicMock
    trace               = MagicMock()
    FastAPIInstrumentor = MagicMock()
    tracer = MagicMock()
    Instrumentator      = MagicMock()


import signal
from contextlib import asynccontextmanager
import time as T
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from option import Some
from ipaddress import IPv4Network
import mictlanxrouter.caching as ChX
import mictlanxrouter.controllers as Cx
# 
from mictlanxrouter.log.logger_config import get_logger
from mictlanx.services import  Summoner


try:
    from mictlanxrouter.middlewares import CPUProfilerMiddleware, MemoryProfilerMiddleware
    
except ImportError as _profiling_import_error:
    class _MissingDependencyMiddleware:
        def __init__(self, *args, **kwargs):
            raise RuntimeError(
                "CPU/Memory profiling middleware requires optional dependencies "
                "(e.g. 'pyinstrument' for CPU profiling and 'memray' for memory "
                "profiling) which are not installed. Install the required "
                "packages to enable profiling."
            ) from _profiling_import_error
    CPUProfilerMiddleware = _MissingDependencyMiddleware
    MemoryProfilerMiddleware = _MissingDependencyMiddleware


L = get_logger(name=config.MICTLANX_ROUTER_LOG_NAME)

if config.MICTLANX_CACHE:
    cache = ChX.CacheFactory.create(
        eviction_policy=config.MICTLANX_CACHE_EVICTION_POLICY,
        capacity=config.MICTLANX_CACHE_CAPACITY,
        capacity_storage=config.MICTLANX_CACHE_CAPACITY_STORAGE
    )
else:
    cache = ChX.NoCache()





# ===========================================================
def signal_handler(sig, frame):
    L.info({
        "EVENT": "SERVICE.SIGNAL",
        "signal": int(sig),
        "message": "Shutting down gracefully...",
    })
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


# _________________________________________



summoner        = Summoner(
    ip_addr     = config.MICTLANX_SUMMONER_IP_ADDR, 
    port        = config.MICTLANX_SUMMONER_PORT, 
    api_version = config.MICTLANX_SUMMONER_API_VERSION,
    network= Some(
        IPv4Network(
            config.MICTLANX_SUMMONER_SUBNET
        )
     )
)


L.debug({
    "EVENT": "SERVICE.CONFIG",
    "MICTLANX_ROUTER_HOST":                         config.MICTLANX_ROUTER_HOST,
    "MICTLANX_ROUTER_PORT":                         config.MICTLANX_ROUTER_PORT,
    "MICTLANX_JEAGER_ENDPOINT":                     config.MICTLANX_JAEGER_ENDPOINT,
    "MICTLANX_ROUTER_OPENTELEMETRY":                config.MICTLANX_ROUTER_OPENTELEMETRY,
    "MICTLANX_ROUTER_LOG_PATH":                            config.MICTLANX_ROUTER_LOG_PATH,
    "MICTLANX_ROUTER_LOG_NAME":                     config.MICTLANX_ROUTER_LOG_NAME,
    "MICTLANX_ROUTER_LOG_INTERVAL":                 config.MICTLANX_ROUTER_LOG_INTERVAL,
    "MICTLANX_ROUTER_LOG_WHEN":                     config.MICTLANX_ROUTER_LOG_WHEN,
    "MICTLANX_ROUTER_LOG_SHOW":                     config.MICTLANX_ROUTER_LOG_SHOW,
    "MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE":     config.MICTLANX_ROUTER_REPLICATOR_QUEUE_MAXSIZE,
    "MICTLANX_ROUTER_SYNC_FAST":                    config.MICTLANX_ROUTER_SYNC_FAST,
    "MICTLANX_ROUTER_MAX_PEERS":                    config.MICTLANX_ROUTER_MAX_PEERS,
    "MICTLANX_ROUTER_MAX_TTL":                      config.MICTLANX_ROUTER_MAX_TTL,
    "MICTLANX_ROUTER_AVAILABLE_NODES":     ";".join(config.MICTLANX_ROUTER_AVAILABLE_NODES),
    "MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS":         config.MICTLANX_ROUTER_SHOW_REPLICATOR_LOGS,
    "MICTLANX_ROUTER_MAX_TRIES":                    config.MICTLANX_ROUTER_MAX_TRIES,
    "MICTLANX_PEERS":                               config.MICTLANX_PEERS_URI,
    "MICTLANX_PROTOCOL":                            config.MICTLANX_PROTOCOL,
    "MICTLANX_API_VERSION":                         config.MICTLANX_API_VERSION,
    "MICTLANX_TIMEOUT":                             config.MICTLANX_TIMEOUT,
})

# ______________________________


@asynccontextmanager
async def lifespan(app:FastAPI):
    t0 = T.time()

    # SERVICE.STARTUP
    L.info({
        "EVENT": "SERVICE.STARTUP",
        "message": "MictlanX Router starting up...",
        "host": config.MICTLANX_ROUTER_HOST,
        "port": config.MICTLANX_ROUTER_PORT,
    })
    try:
        yield
    finally:
        elapsed = T.time() - t0
        L.info({
            "EVENT": "SERVICE.SHUTDOWN",
            "message": "MictlanX Router shutting down...",
            "uptime_seconds": round(elapsed, 2),
        })

peers_controller   = Cx.PeersController(log = L, tracer=tracer, network_id=config.MICTLANX_ROUTER_NETWORK_ID, max_peers_rf=config.MICTLANX_ROUTER_MAX_PEERS_RF)
buckets_controller = Cx.BucketsController(log = L,tracer=tracer,cache = cache)
cache_controller   = Cx.CacheController(log = L, cache =cache,tracer=tracer)

app = FastAPI(
    root_path = os.environ.get("MICTLANX_ROUTER_OPENAPI_PREFIX","/mictlanx"),
    lifespan  = lifespan
    
)
# Open telemetry
FastAPIInstrumentor.instrument_app(app)
Instrumentator().instrument(app).expose(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = [config.MICTLANX_CORS_ALLOW_ORIGINS],
    allow_credentials = config.MICTLANX_CORS_ALLOW_CREDENTIALS,
    allow_methods     = [config.MICTLANX_CORS_ALLOW_METHODS],
    allow_headers     = [config.MICTLANX_CORS_ALLOW_HEADERS]
)


if config.MICTLANX_ROUTER_PROFILER:
    app.add_middleware(
        CPUProfilerMiddleware,
        output_dir        = config.MICTLANX_ROUTER_PROFILER_OUTPUT_DIR,
        enable_by_default = config.MICTLANX_ROUTER_PROFILER_ENABLE_BY_DEFAULT
    )

if config.MICTLANX_ROUTER_MEMORY_PROFILER:
    app.add_middleware(
        MemoryProfilerMiddleware,
        output_dir        = config.MICTLANX_ROUTER_MEMORY_PROFILER_OUTPUT_DIR,
        enable_by_default = config.MICTLANX_ROUTER_MEMORY_PROFILER_ENABLE_BY_DEFAULT,
        report_args       = config.MICTLANX_ROUTER_MEMORY_PROFILER_REPORT_ARGS
    )

def generate_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title       = config.MICTLANX_OPEN_API_TITLE,
        version     = config.MICTLANX_OPEN_API_VERSION,
        summary     = config.MICTLANX_OPEN_API_SUMMARY,
        description = config.MICTLANX_OPEN_API_DESCRIPTION,
        routes      = app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": config.MICTLANX_OPEN_API_LOGO
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema

# 
app.openapi = generate_openapi

app.include_router(peers_controller.router)
app.include_router(buckets_controller.router)
app.include_router(cache_controller.router)



