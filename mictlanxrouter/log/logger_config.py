import logging
from mictlanxrouter.log import Log
from mictlanxrouter import config


def console_handler_filter(lr: logging.LogRecord):
    if config.MICTLANX_ROUTER_DEBUG:
        return True
    return lr.levelno in (logging.INFO, logging.ERROR, logging.WARNING)


def get_logger(name: str):
    
    return Log(
        name                   = name,
        console_handler_filter = console_handler_filter,
        path                   = config.MICTLANX_ROUTER_LOG_PATH
    )

# Logger genérico
L = get_logger("mictlanx-router")
