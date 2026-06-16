from rich.console import Console
from rich.text import Text


class Logger:
  """Pretty, level-free console output.

  Routes everything through a shared Rich Console. There are no log levels,
  prefixes, or timestamps — just clean styled lines:
    - info:    plain text
    - success: green ✓
    - warning: yellow ⚠
    - error:   red ✗
    - debug:   dim, only when verbose

  Messages are rendered as Rich Text (markup disabled) so that content like
  "[3/12]" or bracketed paths prints literally instead of being parsed as markup.
  Use `logger.console.print(...)` directly when you do want Rich markup/renderables.
  """

  def __init__(self):
    self.console = Console()
    self._verbose = True

  def set_verbosity(self, verbose):
    """Toggle non-essential (debug) output. Kept for backwards compatibility."""
    self._verbose = bool(verbose)

  def debug(self, msg):
    if self._verbose:
      self.console.print(Text(str(msg), style="dim"))

  def info(self, msg):
    self.console.print(Text(str(msg)))

  def warning(self, msg):
    self.console.print(Text.assemble(("⚠ ", "yellow"), (str(msg), "yellow")))

  def error(self, msg):
    self.console.print(Text.assemble(("✗ ", "red"), (str(msg), "red")))

  def success(self, msg):
    self.console.print(Text.assemble(("✓ ", "green"), str(msg)))


logger = Logger()
