from loguru import logger as _loguru
from rich.console import Console
from rich.logging import RichHandler


class Logger:
    """Thin wrapper around loguru that routes output through Rich for formatted terminal output."""

    def __init__(self):
        _loguru.remove()
        self.console = Console()
        self._sink_id = None
        self.set_verbosity(True)

    def set_verbosity(self, verbose):
        """Enable or disable log output. When disabled, all log calls are silenced."""
        if self._sink_id is not None:
            try:
                _loguru.remove(self._sink_id)
            except Exception:
                pass
            self._sink_id = None
        if verbose:
            handler = RichHandler(
                rich_tracebacks=True, markup=True, show_path=False, log_time_format="%H:%M:%S"
            )
            self._sink_id = _loguru.add(handler, format="{message}", colorize=True)

    def debug(self, msg):
        _loguru.debug(msg)

    def info(self, msg):
        _loguru.info(msg)

    def warning(self, msg):
        _loguru.warning(msg)

    def error(self, msg):
        _loguru.error(msg)

    def success(self, msg):
        _loguru.success(msg)


logger = Logger()
