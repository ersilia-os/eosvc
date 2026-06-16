from contextlib import contextmanager

from rich.progress import (
  BarColumn,
  DownloadColumn,
  Progress,
  TaskProgressColumn,
  TextColumn,
  TimeRemainingColumn,
  TransferSpeedColumn,
)

from eosvc.logger import logger


class TransferReporter:
  """Reports progress of a multi-file S3 transfer.

  On a TTY, renders a live Rich progress bar (overall %, files done, bytes,
  transfer speed and ETA) sharing the logger's console. Off a TTY (piped or
  redirected output), falls back to one log line per file so that progress is
  still visible in CI logs without garbled Live output.

  Use as a context manager, then call ``file()`` per file to obtain a boto3
  ``Callback``:

      with TransferReporter("Uploading", files) as reporter:
          for path, key, size in files:
              with reporter.file(rel, size) as cb:
                  client.upload_file(path, bucket, key, Callback=cb)
  """

  def __init__(self, action, files):
    """Args:
    action: Human-readable verb, e.g. "Uploading" or "Downloading".
    files: Sequence of items being transferred; only its length and the
        per-call ``size`` matter. Used to compute total counts/bytes.
    """
    self.action = action
    self.action_lower = action.lower()
    self.total_files = len(files)
    self.total_bytes = sum(size or 0 for *_, size in files)
    self.is_tty = logger.console.is_terminal
    self._index = 0
    self._progress = None
    self._task = None

  def __enter__(self):
    if self.is_tty and self.total_files:
      self._progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=logger.console,
      )
      self._progress.start()
      self._task = self._progress.add_task(self.action, total=self.total_bytes)
    return self

  def __exit__(self, exc_type, exc, tb):
    if self._progress is not None:
      self._progress.stop()
      self._progress = None
    return False

  @contextmanager
  def file(self, rel, size):
    """Report a single file, yielding a boto3 ``Callback``.

    Args:
        rel: Relative path/name shown in the description or log line.
        size: File size in bytes (unused off-TTY).

    Yields:
        A callable ``cb(n)`` that advances the overall byte progress by ``n``.
        boto3 invokes it with the byte delta of each chunk. Off-TTY it is a
        no-op and a single line is logged instead.
    """
    self._index += 1
    if self._progress is not None:
      self._progress.update(
        self._task,
        description=f"{self.action} ({self._index}/{self.total_files}) {rel}",
      )

      def cb(n):
        self._progress.update(self._task, advance=n)

      yield cb
    else:
      logger.info(f"[{self._index}/{self.total_files}] {self.action_lower} {rel}")
      yield lambda n: None
