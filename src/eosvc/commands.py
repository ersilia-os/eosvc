import subprocess
from pathlib import Path

from rich.panel import Panel
from rich.text import Text

from eosvc.constants import EOSVCError, BUCKET_PRIVATE
from eosvc.credentials import CREDS, write_home_env
from eosvc.logger import logger
from eosvc.repo import (
  find_repo_root,
  load_access,
  ensure_access_lock,
  repo_name,
  normalize_user_path,
  category_for_path,
  artifacts_plan,
)
from eosvc.s3 import (
  s3_for_read,
  s3_for_write,
  s3_download_path,
  s3_upload_path,
  s3_list_objects,
  s3_resolve_keys,
  s3_delete_keys,
)
from eosvc.view import (
  local_files_map,
  remote_files_map,
  diff_entries,
  render_diff_tree,
  render_object_tree,
  print_legend,
  human_size,
)


def run(cmd, cwd=None):
  """Run a subprocess command, returning stdout on success.

  Args:
      cmd: List of command arguments.
      cwd: Optional working directory.

  Returns:
      stdout string.

  Raises:
      EOSVCError: If the command is not found or exits with a non-zero return code.
  """
  try:
    p = subprocess.run(
      cmd,
      cwd=str(cwd) if cwd else None,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      text=True,
    )
  except FileNotFoundError as e:
    raise EOSVCError(f"Command not found: {cmd[0]}") from e
  if p.returncode:
    msg = f"Command failed ({p.returncode}): {' '.join(cmd)}\n"
    if p.stdout.strip():
      msg += f"\nSTDOUT:\n{p.stdout}"
    if p.stderr.strip():
      msg += f"\nSTDERR:\n{p.stderr}"
    raise EOSVCError(msg)
  return p.stdout


def cmd_config(args):
  """Write AWS credentials to ~/.eosvc/.env from CLI arguments."""
  akid = (args.access_key_id or "").strip()
  sak = (args.secret_access_key or "").strip()
  if not akid or not sak:
    raise EOSVCError("--access-key-id and --secret-access-key are required")
  write_home_env(
    access_key_id=akid,
    secret_access_key=sak,
    session_token=(args.session_token or "").strip() or None,
    region=(args.region or "").strip() or None,
    default_region=(args.default_region or "").strip() or None,
  )
  CREDS.reset()
  logger.success("Wrote credentials to ~/.eosvc/.env")


def cmd_download(args):
  """Download one or all artifact paths for the current repo from S3.

  Locates the repo root by searching for access.json upward from cwd.
  Use --path . to download all managed artifact directories.
  """
  repo_dir = find_repo_root(Path.cwd())
  policy, mode = load_access(repo_dir)
  ensure_access_lock(repo_dir, policy, mode)
  repo = repo_name(repo_dir)

  rel_path_raw = (args.path or "").strip().lstrip("/")

  if rel_path_raw in {"", ".", "./"}:
    for rel_dir, cat in artifacts_plan(mode):
      bucket = policy.bucket_for(cat)
      if bucket == BUCKET_PRIVATE:
        CREDS.resolve(repo_dir=repo_dir, require=True)
      client = s3_for_read(bucket, repo_dir)
      logger.info(f"Downloading --path {rel_dir} from s3://{bucket}/{repo}/")
      try:
        s3_download_path(client, bucket, repo, rel_dir, repo_dir)
      except EOSVCError as e:
        if "AccessDenied" in str(e):
          _, source, arn = CREDS.resolve(repo_dir=repo_dir, require=False)
          hint = (
            f" (credentials source: {source}, principal: {arn})" if source else " (no credentials)"
          )
          raise EOSVCError(str(e) + hint)
        raise
    logger.success("Download complete.")
    return

  rel_path = normalize_user_path(rel_path_raw, mode)
  cat = category_for_path(rel_path, mode)
  bucket = policy.bucket_for(cat)
  if bucket == BUCKET_PRIVATE:
    CREDS.resolve(repo_dir=repo_dir, require=True)

  client = s3_for_read(bucket, repo_dir)
  logger.info(f"Downloading --path {rel_path} from s3://{bucket}/{repo}/")
  try:
    s3_download_path(client, bucket, repo, rel_path, repo_dir)
  except EOSVCError as e:
    if "AccessDenied" in str(e):
      _, source, arn = CREDS.resolve(repo_dir=repo_dir, require=False)
      hint = f" (credentials source: {source}, principal: {arn})" if source else " (no credentials)"
      raise EOSVCError(str(e) + hint)
    raise
  logger.success("Download complete.")


def cmd_upload(args):
  """Upload one or all artifact paths for the current repo to S3.

  Locates the repo root by searching for access.json upward from cwd.
  Use --path . to upload all managed artifact directories that exist locally.
  """
  repo_dir = find_repo_root(Path.cwd())
  policy, mode = load_access(repo_dir)
  ensure_access_lock(repo_dir, policy, mode)
  repo = repo_name(repo_dir)

  rel_path_raw = (args.path or "").strip().lstrip("/")

  if rel_path_raw in {"", ".", "./"}:
    for rel_dir, cat in artifacts_plan(mode):
      local_dir = repo_dir / rel_dir
      if not local_dir.exists():
        continue
      bucket = policy.bucket_for(cat)
      client = s3_for_write(bucket, repo_dir)
      logger.info(f"Uploading --path {rel_dir} to s3://{bucket}/{repo}/")
      try:
        s3_upload_path(client, bucket, repo, Path(rel_dir), repo_dir)
      except EOSVCError as e:
        if "AccessDenied" in str(e):
          _, source, arn = CREDS.resolve(repo_dir=repo_dir, require=False)
          raise EOSVCError(str(e) + f" (credentials source: {source}, principal: {arn})")
        raise
    logger.success("Upload complete.")
    return

  rel_path = normalize_user_path(rel_path_raw, mode)
  cat = category_for_path(rel_path, mode)
  bucket = policy.bucket_for(cat)

  client = s3_for_write(bucket, repo_dir)
  logger.info(f"Uploading --path {rel_path} to s3://{bucket}/{repo}/")
  try:
    s3_upload_path(client, bucket, repo, Path(rel_path), repo_dir)
  except EOSVCError as e:
    if "AccessDenied" in str(e):
      _, source, arn = CREDS.resolve(repo_dir=repo_dir, require=False)
      raise EOSVCError(str(e) + f" (credentials source: {source}, principal: {arn})")
    raise
  logger.success("Upload complete.")


def cmd_view(args):
  """Display the S3 directory tree for one or all artifact paths in the current repo.

  Locates the repo root by searching for access.json upward from cwd.
  Use --path . to view all managed artifact directories.
  """
  repo_dir = find_repo_root(Path.cwd())
  policy, mode = load_access(repo_dir)
  ensure_access_lock(repo_dir, policy, mode)
  repo = repo_name(repo_dir)

  print_legend(logger.console)

  rel_path = (args.path or ".").strip().lstrip("/")
  if rel_path in {"", "."}:
    totals = []
    for rel_dir, cat in artifacts_plan(mode):
      bucket = policy.bucket_for(cat)
      if bucket == BUCKET_PRIVATE:
        CREDS.resolve(repo_dir=repo_dir, require=True)
      client = s3_for_read(bucket, repo_dir)
      prefix = f"{repo}/{rel_dir}/"
      objects = s3_list_objects(client, bucket, prefix)
      remote = remote_files_map(objects, f"{repo}/")
      local = local_files_map(repo_dir, rel_dir)
      entries = diff_entries(local, remote)
      counts = render_diff_tree(
        logger.console,
        f"{rel_dir}  (s3://{bucket}/{repo}/)",
        entries,
        strip=rel_dir,
        max_depth=args.max_depth,
      )
      totals.append(counts)
    differ = sum(c["differ"] for c in totals)
    same = sum(c["same size"] for c in totals)
    logger.console.print()
    logger.console.print(
      f"[bold]Summary:[/bold] {differ} differ · {same} same size across {len(totals)} categories"
    )
    return

  rel_path = normalize_user_path(rel_path, mode)
  cat = category_for_path(rel_path, mode)
  bucket = policy.bucket_for(cat)
  if bucket == BUCKET_PRIVATE:
    CREDS.resolve(repo_dir=repo_dir, require=True)
  client = s3_for_read(bucket, repo_dir)
  rel_dir = rel_path.rstrip("/")
  prefix = f"{repo}/{rel_dir}/"
  objects = s3_list_objects(client, bucket, prefix)
  remote = remote_files_map(objects, f"{repo}/")
  local = local_files_map(repo_dir, rel_dir)
  entries = diff_entries(local, remote)
  render_diff_tree(
    logger.console,
    f"{rel_dir}  (s3://{bucket}/{repo}/)",
    entries,
    strip=rel_dir,
    max_depth=args.max_depth,
  )


def _print_delete_warning(console, n_files, n_bytes):
  """Print a bold-red destructive-action banner reminding the user to coordinate."""
  body = Text()
  body.append("⚠ DESTRUCTIVE: ", style="bold red")
  body.append("this permanently removes ")
  body.append(f"{n_files} file(s) ", style="bold")
  body.append("(")
  body.append_text(human_size(n_bytes))
  body.append(") from S3.\n")
  body.append(
    "These artifacts are shared — coordinate with your teammates before deleting.\n",
    style="yellow",
  )
  body.append("Local files are NOT affected; this only deletes the remote copy.", style="dim")
  console.print()
  console.print(Panel(body, border_style="red", title="[bold red]Delete[/bold red]", expand=False))


def cmd_delete(args):
  """Delete one or all artifact paths for the current repo from S3 (remote only).

  Locates the repo root by searching for access.json upward from cwd. Shows exactly
  what will be removed, warns loudly, and requires typed confirmation before deleting.
  Local files are never touched. Use --path . to target all managed directories.
  """
  repo_dir = find_repo_root(Path.cwd())
  policy, mode = load_access(repo_dir)
  ensure_access_lock(repo_dir, policy, mode)
  repo = repo_name(repo_dir)

  rel_path_raw = (args.path or "").strip().lstrip("/")
  bulk = rel_path_raw in {"", ".", "./"}

  # Resolve targets up front (no deletion yet): list of (label, bucket, client, objects).
  targets = []
  if bulk:
    for rel_dir, cat in artifacts_plan(mode):
      bucket = policy.bucket_for(cat)
      client = s3_for_write(bucket, repo_dir)
      objects = s3_resolve_keys(client, bucket, repo, rel_dir)
      targets.append((rel_dir, bucket, client, objects))
    rel_path = None
  else:
    rel_path = normalize_user_path(rel_path_raw, mode)
    cat = category_for_path(rel_path, mode)
    bucket = policy.bucket_for(cat)
    client = s3_for_write(bucket, repo_dir)
    objects = s3_resolve_keys(client, bucket, repo, rel_path)
    if not objects:
      raise EOSVCError(f"Nothing found at s3://{bucket}/{repo}/{rel_path}")
    targets.append((rel_path, bucket, client, objects))

  # Preview everything that would be deleted.
  total_files = 0
  total_bytes = 0
  for label, bucket, client, objects in targets:
    n_files, n_bytes = render_object_tree(
      logger.console,
      f"WILL DELETE — {label}  (s3://{bucket}/{repo}/)",
      objects,
      f"{repo}/",
      strip=label,
      max_depth=args.max_depth,
    )
    total_files += n_files
    total_bytes += n_bytes

  if total_files == 0:
    logger.console.print()
    logger.console.print("Nothing to delete.")
    return

  _print_delete_warning(logger.console, total_files, total_bytes)

  # Typed confirmation (path for a subpath delete, repo name for --path .).
  if not args.yes:
    if not logger.console.is_terminal:
      raise EOSVCError("Refusing to delete without confirmation; run in a terminal or pass --yes.")
    if bulk:
      expected = repo
      answer = logger.console.input(f"Type the repository name '{repo}' to confirm: ")
    else:
      expected = rel_path
      answer = logger.console.input(f"Type the path '{rel_path}' to confirm: ")
    if answer.strip() != expected:
      logger.console.print("Aborted (confirmation did not match).")
      return

  # Delete.
  deleted = 0
  for label, bucket, client, objects in targets:
    keys = [o["key"] for o in objects]
    if not keys:
      continue
    logger.info(f"Deleting --path {label} from s3://{bucket}/{repo}/")
    try:
      deleted += s3_delete_keys(client, bucket, keys)
    except EOSVCError as e:
      if "AccessDenied" in str(e):
        _, source, arn = CREDS.resolve(repo_dir=repo_dir, require=False)
        raise EOSVCError(str(e) + f" (credentials source: {source}, principal: {arn})")
      raise
  logger.success(f"Deleted {deleted} object(s). Run 'eosvc view' to confirm.")
