import subprocess
from pathlib import Path

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
    s3_print_tree,
    s3_list_keys,
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
                        f" (credentials source: {source}, principal: {arn})"
                        if source
                        else " (no credentials)"
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
            hint = (
                f" (credentials source: {source}, principal: {arn})"
                if source
                else " (no credentials)"
            )
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
                    raise EOSVCError(
                        str(e) + f" (credentials source: {source}, principal: {arn})"
                    )
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

    rel_path = (args.path or ".").strip().lstrip("/")
    if rel_path in {"", "."}:
        for rel_dir, cat in artifacts_plan(mode):
            bucket = policy.bucket_for(cat)
            if bucket == BUCKET_PRIVATE:
                CREDS.resolve(repo_dir=repo_dir, require=True)
            client = s3_for_read(bucket, repo_dir)
            prefix = f"{repo}/{rel_dir}/"
            logger.info(f"[{rel_dir}] s3://{bucket}/{prefix}")
            s3_print_tree(s3_list_keys(client, bucket, prefix), prefix)
            logger.info("")
        return

    rel_path = normalize_user_path(rel_path, mode)
    cat = category_for_path(rel_path, mode)
    bucket = policy.bucket_for(cat)
    if bucket == BUCKET_PRIVATE:
        CREDS.resolve(repo_dir=repo_dir, require=True)
    client = s3_for_read(bucket, repo_dir)
    prefix = f"{repo}/{rel_path.rstrip('/')}/"
    logger.info(f"s3://{bucket}/{prefix}")
    s3_print_tree(s3_list_keys(client, bucket, prefix), prefix)
