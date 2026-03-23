#!/usr/bin/env python3

import builtins
import logging
import os
import re
import sys
from typing import Any


_CONFIGURED = False


class _ColorFormatter(logging.Formatter):
    _RESET = "\033[0m"
    _LEVEL_COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[94m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[1;31m",
    }

    def format(self, record: logging.LogRecord) -> str:
        level_color = self._LEVEL_COLORS.get(record.levelno, "")
        level_name = f"{level_color}{record.levelname:<8}{self._RESET}" if level_color else f"{record.levelname:<8}"
        timestamp = self.formatTime(record, self.datefmt)
        message = record.getMessage()
        return f"{timestamp} {level_name} [{record.name}] {message}"


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    level_name = os.getenv("FIREFLY_LOG_LEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_ColorFormatter(datefmt="%H:%M:%S"))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)

    for logger_name in ("engineio", "engineio.server", "socketio", "socketio.server", "werkzeug"):
        library_logger = logging.getLogger(logger_name)
        library_logger.handlers.clear()
        library_logger.propagate = True
        if library_logger.level == logging.NOTSET:
            library_logger.setLevel(level)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


def _infer_level(message: str) -> int:
    lowered = message.lower()
    if "[recv]" in lowered:
        return logging.INFO
    if "[nodeinfo_debug]" in lowered:
        return logging.DEBUG
    if (
        "failed" in lowered
        or "exception" in lowered
        or "traceback" in lowered
        or "❌" in lowered
        or "[error]" in lowered
        or re.search(r"\berror\b", lowered)
    ):
        return logging.ERROR
    if any(token in lowered for token in ("warning", "warn", "conflict", "[warn]")):
        return logging.WARNING
    if "debug" in lowered:
        return logging.DEBUG
    return logging.INFO


def make_log_print(logger: logging.Logger):
    def _log_print(*args: Any, sep: str = " ", end: str = "\n", **kwargs: Any) -> None:
        if kwargs.get("file") not in (None, sys.stdout, sys.stderr):
            return builtins.print(*args, sep=sep, end=end, **kwargs)

        message = sep.join(str(arg) for arg in args)
        if end and end != "\n":
            message = f"{message}{end.rstrip()}"
        logger.log(_infer_level(message), message)

    return _log_print
