"""Context tags for stdlib and Pipecat (loguru) logs."""
import contextvars
import logging

from loguru import logger as pipecat_logger

device_tag = contextvars.ContextVar("device_tag", default="-")
_installed = False


def install_logging():
    global _installed
    if _installed:
        return
    _installed = True
    previous = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = previous(*args, **kwargs)
        record.device = device_tag.get()
        return record

    logging.setLogRecordFactory(record_factory)
    for handler in logging.getLogger().handlers:
        formatter = handler.formatter
        if formatter and "%(device)" not in formatter._fmt:
            handler.setFormatter(logging.Formatter(
                formatter._fmt.replace("%(name)", "[%(device)s] %(name)", 1),
                datefmt=formatter.datefmt,
            ))

    def patch(record):
        tag = device_tag.get()
        record["extra"]["device"] = tag
        if tag != "-":
            record["message"] = f"[{tag}] {record['message']}"

    pipecat_logger.configure(patcher=patch)
