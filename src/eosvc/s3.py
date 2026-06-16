import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from eosvc.constants import EOSVCError, BUCKET_PUBLIC, BUCKET_MODELS_PUBLIC
from eosvc.credentials import CREDS, env_region
from eosvc.logger import logger
from eosvc.progress import TransferReporter


def iter_local_files(path):
  """Yield all files under path. If path is a file, yields it directly."""
  if path.is_file():
    yield path
    return
  for p in path.rglob("*"):
    if p.is_file():
      yield p


def s3_unsigned():
  """Return an anonymous (unsigned) S3 client for public bucket reads."""
  return boto3.client("s3", region_name=env_region(), config=Config(signature_version=UNSIGNED))


def s3_for_read(bucket, repo_dir):
  """Return an S3 client suitable for reading from the given bucket.

  For public buckets, uses an unsigned client if no credentials are available.
  For private buckets, requires valid credentials.

  Args:
      bucket: S3 bucket name.
      repo_dir: Repo root path, passed to credential resolution for .env lookup.

  Raises:
      EOSVCError: If the bucket is private and no credentials are found.
  """
  if bucket in (BUCKET_PUBLIC, BUCKET_MODELS_PUBLIC):
    return s3_unsigned()
  session, _, _ = CREDS.resolve(repo_dir=repo_dir, require=True)
  return session.client("s3", region_name=env_region())


def s3_for_write(bucket, repo_dir):
  """Return an authenticated S3 client for writing to the given bucket.

  Always requires valid credentials regardless of bucket visibility.

  Args:
      bucket: S3 bucket name.
      repo_dir: Repo root path, passed to credential resolution for .env lookup.

  Raises:
      EOSVCError: If no valid credentials are found.
  """
  session, source, arn = CREDS.resolve(repo_dir=repo_dir, require=True)
  if session is None:
    raise EOSVCError("AWS credentials required for upload.")
  logger.info(f"Using AWS credentials from: {source} ({arn})")
  return session.client("s3", region_name=env_region())


def s3_list_keys(client, bucket, prefix):
  """List all S3 keys under prefix, paginating through results automatically.

  Args:
      client: Boto3 S3 client.
      bucket: S3 bucket name.
      prefix: Key prefix to list.

  Returns:
      List of key strings.

  Raises:
      EOSVCError: On S3 API errors.
  """
  keys = []
  token = None
  try:
    while True:
      kwargs = {"Bucket": bucket, "Prefix": prefix}
      if token:
        kwargs["ContinuationToken"] = token
      resp = client.list_objects_v2(**kwargs)
      for obj in resp.get("Contents") or []:
        keys.append(obj["Key"])
      if not resp.get("IsTruncated"):
        break
      token = resp.get("NextContinuationToken")
  except (BotoCoreError, ClientError) as e:
    raise EOSVCError(f"S3 error listing s3://{bucket}/{prefix}: {e}") from e
  return keys


def s3_list_objects(client, bucket, prefix):
  """List all S3 objects under prefix, paginating automatically.

  Like s3_list_keys but also returns each object's size and upload date (used to
  drive transfer progress and the local/remote diff in `view`).

  Args:
      client: Boto3 S3 client.
      bucket: S3 bucket name.
      prefix: Key prefix to list.

  Returns:
      List of dicts with keys 'key' (str), 'size' (int), and 'last_modified'
      (tz-aware datetime or None).

  Raises:
      EOSVCError: On S3 API errors.
  """
  objects = []
  token = None
  try:
    while True:
      kwargs = {"Bucket": bucket, "Prefix": prefix}
      if token:
        kwargs["ContinuationToken"] = token
      resp = client.list_objects_v2(**kwargs)
      for obj in resp.get("Contents") or []:
        objects.append({
          "key": obj["Key"],
          "size": obj.get("Size", 0),
          "last_modified": obj.get("LastModified"),
        })
      if not resp.get("IsTruncated"):
        break
      token = resp.get("NextContinuationToken")
  except (BotoCoreError, ClientError) as e:
    raise EOSVCError(f"S3 error listing s3://{bucket}/{prefix}: {e}") from e
  return objects


def s3_resolve_keys(client, bucket, repo_prefix, rel_path):
  """Resolve a repo-relative path to the S3 objects it points at.

  Tries rel_path as an exact key first; if not found, treats it as a directory
  prefix. This mirrors the resolution in s3_download_path and is the single source
  of truth for "what does this path point at" (used by both the delete preview and
  the deletion itself).

  Args:
      client: Boto3 S3 client.
      bucket: S3 bucket name.
      repo_prefix: Repo-level S3 prefix (i.e. the repo name).
      rel_path: Relative path within the repo (e.g. 'data/inputs/file.csv').

  Returns:
      List of object dicts ({key, size, last_modified}); empty if nothing matches.

  Raises:
      EOSVCError: On S3 API errors.
  """
  rel_path = rel_path.strip().lstrip("/")
  base = repo_prefix.rstrip("/") + "/"
  file_key = base + rel_path
  dir_prefix = base + rel_path.rstrip("/") + "/"

  exact = [o for o in s3_list_objects(client, bucket, file_key) if o["key"] == file_key]
  if exact:
    return exact
  return [o for o in s3_list_objects(client, bucket, dir_prefix) if not o["key"].endswith("/")]


def s3_delete_keys(client, bucket, keys):
  """Delete the given S3 keys in batches, returning the number deleted.

  Batches keys into groups of 1000 (the delete_objects limit) and logs progress.

  Args:
      client: Boto3 S3 client.
      bucket: S3 bucket name.
      keys: List of full S3 key strings to delete.

  Returns:
      Number of objects deleted.

  Raises:
      EOSVCError: If S3 reports any per-key error, or on S3 API errors.
  """
  deleted = 0
  total = len(keys)
  try:
    for start in range(0, total, 1000):
      batch = keys[start : start + 1000]
      resp = client.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
      )
      errors = resp.get("Errors") or []
      if errors:
        first = errors[0]
        raise EOSVCError(
          f"S3 delete failed s3://{bucket}/{first.get('Key')}: "
          f"{first.get('Code')} {first.get('Message')}"
        )
      deleted += len(batch)
      logger.info(f"Deleted {deleted}/{total} object(s)")
  except (BotoCoreError, ClientError) as e:
    raise EOSVCError(f"S3 error deleting from s3://{bucket}/: {e}") from e
  return deleted


def s3_download_path(client, bucket, repo_prefix, rel_path, repo_dir):
  """Download a single file or all files under a prefix from S3 to the local repo.

  Tries rel_path as an exact key first; if not found, treats it as a directory prefix
  and downloads all matching keys.

  Args:
      client: Boto3 S3 client.
      bucket: S3 bucket name.
      repo_prefix: Repo-level S3 prefix (i.e. the repo name).
      rel_path: Relative path within the repo (e.g. 'data/inputs/file.csv').
      repo_dir: Local repo root Path where files will be written.

  Raises:
      EOSVCError: If nothing is found at the given path, or on download failure.
  """
  rel_path = rel_path.strip().lstrip("/")
  base = repo_prefix.rstrip("/") + "/"
  file_key = base + rel_path
  dir_prefix = base + rel_path.rstrip("/") + "/"

  # Build the list of objects to download (key, size), trying the exact file
  # first, then falling back to a directory prefix.
  exact = [
    (o["key"], o["size"]) for o in s3_list_objects(client, bucket, file_key) if o["key"] == file_key
  ]
  if exact:
    objects = exact
  else:
    objects = [
      (o["key"], o["size"])
      for o in s3_list_objects(client, bucket, dir_prefix)
      if not o["key"].endswith("/")
    ]
  if not objects:
    raise EOSVCError(f"Nothing found at s3://{bucket}/{file_key} or s3://{bucket}/{dir_prefix}")

  targets = [(key, repo_dir / key[len(base) :].lstrip("/"), size) for key, size in objects]

  with TransferReporter("Downloading", targets) as reporter:
    for key, dest, size in targets:
      rel = key[len(base) :].lstrip("/")
      dest.parent.mkdir(parents=True, exist_ok=True)
      try:
        with reporter.file(rel, size) as cb:
          client.download_file(bucket, key, str(dest), Callback=cb)
      except (BotoCoreError, ClientError) as e:
        raise EOSVCError(f"S3 download failed s3://{bucket}/{key}: {e}") from e


def s3_upload_path(client, bucket, repo_prefix, src_path, repo_dir):
  """Upload a local file or all files under a local directory to S3.

  Args:
      client: Boto3 S3 client.
      bucket: S3 bucket name.
      repo_prefix: Repo-level S3 prefix (i.e. the repo name).
      src_path: Path to the local file or directory to upload.
      repo_dir: Local repo root Path, used to compute relative S3 keys.

  Raises:
      EOSVCError: If src_path does not exist, or on upload failure.
  """
  src_path = (repo_dir / src_path).resolve() if not src_path.is_absolute() else src_path.resolve()
  if not src_path.exists():
    raise EOSVCError(f"Path does not exist: {src_path}")

  repo_dir_abs = repo_dir.resolve()
  targets = []
  for file_path in iter_local_files(src_path):
    rel = file_path.relative_to(repo_dir_abs).as_posix()
    key = f"{repo_prefix.rstrip('/')}/{rel}"
    targets.append((file_path, key, rel, file_path.stat().st_size))

  with TransferReporter("Uploading", [(p, k, s) for p, k, _, s in targets]) as reporter:
    for file_path, key, rel, size in targets:
      try:
        with reporter.file(rel, size) as cb:
          client.upload_file(str(file_path), bucket, key, Callback=cb)
      except (BotoCoreError, ClientError) as e:
        raise EOSVCError(f"S3 upload failed {file_path} -> s3://{bucket}/{key}: {e}") from e
