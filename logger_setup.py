"""
Centralized logging configuration for the SAP Telegram Bot.

Provides structured JSON logging for production observability and
a human-readable console format for development.

Usage in any module:
    from logger_setup import get_logger
    logger = get_logger(__name__)
    logger.info("Order created", extra={"sap_order_id": "12345", "user_id": 999})
"""

import logging
from logging.handlers import RotatingFileHandler
import json
import sys
import os
from datetime import datetime, timezone
from typing import Optional

# Lazy-loaded PII masker to avoid circular imports
_pii_masker = None

def _get_masker():
    global _pii_masker
    if _pii_masker is None:
        try:
            from pii_masker import PIIMasker
            _pii_masker = PIIMasker()
        except ImportError:
            _pii_masker = None
    return _pii_masker


class StructuredFormatter(logging.Formatter):
    """JSON structured log formatter with PII masking for production environments."""

    def format(self, record: logging.LogRecord) -> str:
        masker = _get_masker()
        message = record.getMessage()
        if masker:
            message = masker.mask(message)

        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Include extra fields passed via logger.info(..., extra={...})
        standard_attrs = {
            "name", "msg", "args", "created", "relativeCreated",
            "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "pathname", "filename", "module", "levelno", "levelname",
            "message", "msecs", "thread", "threadName", "process",
            "processName", "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                # Mask extra field values too
                if masker and isinstance(value, str):
                    log_entry[key] = masker.mask(value)
                else:
                    log_entry[key] = value

        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-readable console formatter for development."""

    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[1;31m",  # Bold Red
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        masker = _get_masker()
        color = self.COLORS.get(record.levelname, self.RESET)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = record.getMessage()
        if masker:
            message = masker.mask(message)

        base = f"{color}{timestamp} [{record.levelname:8s}]{self.RESET} {record.name}: {message}"

        # Append extra fields if present
        standard_attrs = {
            "name", "msg", "args", "created", "relativeCreated",
            "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "pathname", "filename", "module", "levelno", "levelname",
            "message", "msecs", "thread", "threadName", "process",
            "processName", "taskName",
        }
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in standard_attrs and not k.startswith("_")
        }
        if extras:
            masked_extras = {}
            for k, v in extras.items():
                masked_extras[k] = masker.mask(str(v)) if masker and isinstance(v, str) else v
            extra_str = " | ".join(f"{k}={v}" for k, v in masked_extras.items())
            base += f"  [{extra_str}]"

        if record.exc_info and record.exc_info[0] is not None:
            base += "\n" + self.formatException(record.exc_info)

        return base


def setup_logging(log_level: Optional[str] = None) -> None:
    """
    Configure the root logger for the entire application.

    Call this ONCE at application startup (in bot.py main block).

    Args:
        log_level: Override log level. Defaults to LOG_LEVEL env var or INFO.
    """
    level_name = log_level or os.environ.get("LOG_LEVEL", "INFO")
    level = getattr(logging, level_name.upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear any existing handlers (prevents duplicate logs on re-init)
    root_logger.handlers.clear()

    # Console handler (human-readable)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(ConsoleFormatter())
    root_logger.addHandler(console_handler)

    # File handler (JSON structured, for production log aggregation)
    log_dir = os.environ.get("LOG_DIR", ".")
    log_file = os.path.join(log_dir, "sap_bot.log")
    try:
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(StructuredFormatter())
        root_logger.addHandler(file_handler)
    except (OSError, PermissionError):
        root_logger.warning("Could not create log file at %s, file logging disabled", log_file)

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a named logger instance.

    Args:
        name: Typically __name__ of the calling module.

    Returns:
        Configured logging.Logger instance.
    """
    return logging.getLogger(name)
