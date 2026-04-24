from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from eosvc.constants import EOSVCError, BUCKET_PUBLIC
from eosvc.credentials import CREDS, env_region
from eosvc.logger import logger


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
    if bucket == BUCKET_PUBLIC:
        session, _, _ = CREDS.resolve(repo_dir=repo_dir, require=False)
        if session is None:
            return s3_unsigned()
        return session.client("s3", region_name=env_region())
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

    exact = [k for k in s3_list_keys(client, bucket, file_key) if k == file_key]
    if exact:
        dest = repo_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            client.download_file(bucket, file_key, str(dest))
        except (BotoCoreError, ClientError) as e:
            raise EOSVCError(f"S3 download failed s3://{bucket}/{file_key}: {e}") from e
        return

    keys = [k for k in s3_list_keys(client, bucket, dir_prefix) if not k.endswith("/")]
    if not keys:
        raise EOSVCError(
            f"Nothing found at s3://{bucket}/{file_key} or s3://{bucket}/{dir_prefix}"
        )

    for key in keys:
        rel = key[len(base):].lstrip("/")
        dest = repo_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            client.download_file(bucket, key, str(dest))
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
    for file_path in iter_local_files(src_path):
        rel = file_path.relative_to(repo_dir_abs).as_posix()
        key = f"{repo_prefix.rstrip('/')}/{rel}"
        try:
            client.upload_file(str(file_path), bucket, key)
        except (BotoCoreError, ClientError) as e:
            raise EOSVCError(f"S3 upload failed {file_path} -> s3://{bucket}/{key}: {e}") from e


def s3_print_tree(keys, base_prefix):
    """Print S3 keys under base_prefix as an ASCII directory tree.

    Args:
        keys: List of S3 key strings.
        base_prefix: The prefix to strip from keys before rendering.
    """
    base_prefix = base_prefix.rstrip("/") + "/"
    rels = []
    for k in keys:
        if k.startswith(base_prefix):
            rel = k[len(base_prefix):].lstrip("/")
            if rel:
                rels.append(rel)

    if not rels:
        logger.info("(empty)")
        return

    tree = {}
    for rel in rels:
        parts = [p for p in rel.split("/") if p]
        node = tree
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        node.setdefault(parts[-1], {})

    def walk(node, prefix=""):
        items = sorted(node.items(), key=lambda x: x[0])
        for i, (name, child) in enumerate(items):
            last = i == len(items) - 1
            logger.info(prefix + ("└── " if last else "├── ") + name)
            if child:
                walk(child, prefix + ("    " if last else "│   "))

    walk(tree)
