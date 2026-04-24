import os
from pathlib import Path

import boto3

from eosvc.constants import EOSVCError, EOSVC_HOME_DIR, EOSVC_HOME_ENV
from eosvc.logger import logger


def env_region():
    """Return the AWS region from environment variables, defaulting to us-east-1."""
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"


class CredManager:
    """Resolves AWS credentials using a layered strategy.

    Resolution order:
    1. .env files in .config/ subdirs of the repo or cwd (loaded via python-dotenv)
    2. AWS default credential chain (environment variables and ~/.aws/)
    3. Falls back to None (anonymous) — only valid for public bucket reads

    Credentials are validated via sts:GetCallerIdentity. Results are cached after
    the first successful call; use reset() to force re-resolution.
    """

    def __init__(self):
        self._session = None
        self._source = None
        self._caller_arn = None
        self._checked = False

    def reset(self):
        """Clear cached credentials and force re-resolution on the next call."""
        self._session = None
        self._source = None
        self._caller_arn = None
        self._checked = False

    def _try_sts(self, session):
        try:
            sts = session.client("sts", region_name=env_region())
            ident = sts.get_caller_identity()
            arn = ident.get("Arn")
            return arn or "<unknown-arn>"
        except Exception:
            return None

    def _dotenv_paths(self, repo_dir):
        paths = []
        if repo_dir:
            paths.append(Path(repo_dir) / ".config" / ".env")
            paths.append(Path(repo_dir) / ".config" / "eosvc" / ".env")
        paths.append(Path.cwd() / ".config" / ".env")
        paths.append(Path.cwd() / ".config" / "eosvc" / ".env")
        paths.append(EOSVC_HOME_ENV)
        if repo_dir:
            paths.append(Path(repo_dir) / ".env")
        paths.append(Path.cwd() / ".env")
        seen = set()
        out = []
        for p in paths:
            rp = str(p.resolve()) if p.exists() else str(p)
            if rp not in seen:
                seen.add(rp)
                out.append(p)
        return out

    def _load_dotenv(self, repo_dir):
        try:
            from dotenv import load_dotenv
        except Exception:
            raise EOSVCError(
                "python-dotenv is required for .env fallback. Install: pip install python-dotenv"
            )
        loaded_any = False
        for p in self._dotenv_paths(repo_dir):
            if p.exists():
                load_dotenv(dotenv_path=str(p), override=True)
                loaded_any = True
        return loaded_any

    def _has_aws_files(self):
        home = Path.home()
        return (home / ".aws" / "credentials").exists() or (home / ".aws" / "config").exists()

    def resolve(self, repo_dir=None, require=False):
        """Resolve AWS credentials, returning (session, source, caller_arn).

        Args:
            repo_dir: Path to the repo root, used to locate .env files.
            require: If True, raises EOSVCError when no valid credentials are found.

        Returns:
            Tuple of (boto3.Session, source_description, caller_arn).
            All three are None if no credentials are found and require=False.

        Raises:
            EOSVCError: If require=True and no valid credentials are found.
        """
        if self._checked:
            if require and self._session is None:
                raise EOSVCError(self._missing_message(repo_dir))
            return self._session, self._source, self._caller_arn

        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

        loaded = self._load_dotenv(repo_dir)
        if loaded:
            session0 = boto3.Session(region_name=env_region())
            creds0 = session0.get_credentials()
            if creds0:
                arn0 = self._try_sts(session0)
                if arn0:
                    self._session = session0
                    self._source = ".env (.config first, python-dotenv)"
                    self._caller_arn = arn0
                    self._checked = True
                    return self._session, self._source, self._caller_arn
                logger.warning(
                    "Loaded .env (.config first) but credentials failed validation (sts:GetCallerIdentity). Trying AWS default chain."
                )
            else:
                logger.warning(
                    "Loaded .env (.config first) but boto3 did not resolve credentials. Trying AWS default chain."
                )
        else:
            logger.warning("No .env found in .config locations; trying AWS default chain.")

        session = boto3.Session(region_name=env_region())
        creds = session.get_credentials()
        if creds:
            arn = self._try_sts(session)
            if arn:
                self._session = session
                self._source = "aws-default-chain (env and/or ~/.aws)"
                self._caller_arn = arn
                self._checked = True
                return self._session, self._source, self._caller_arn
            logger.warning(
                "AWS credentials found (env and/or ~/.aws) but validation failed (sts:GetCallerIdentity)."
            )
        else:
            if self._has_aws_files():
                logger.warning(
                    "Found ~/.aws credentials/config files but boto3 did not resolve credentials."
                )
            else:
                logger.warning("No AWS credentials found in env or ~/.aws.")

        self._session = None
        self._source = None
        self._caller_arn = None
        self._checked = True

        if require:
            raise EOSVCError(self._missing_message(repo_dir))
        return None, None, None

    def _missing_message(self, repo_dir):
        env_hint = (
            "Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (and optional AWS_SESSION_TOKEN), "
            "or configure AWS CLI (aws configure), or run 'eosvc config' to write ~/.eosvc/.env."
        )
        searched = [str(p) for p in self._dotenv_paths(repo_dir)]
        return (
            "AWS credentials are missing or invalid.\n"
            f"- Checked .env paths (including .config first): {', '.join(searched)}\n"
            f"- Checked: AWS default chain (env and ~/.aws)\n"
            f"- Fix: {env_hint}"
        )


CREDS = CredManager()


def write_home_env(
    access_key_id, secret_access_key, session_token=None, region=None, default_region=None
):
    """Write AWS credentials to ~/.eosvc/.env.

    Args:
        access_key_id: AWS access key ID.
        secret_access_key: AWS secret access key.
        session_token: Optional session token for temporary credentials.
        region: Optional AWS_REGION value.
        default_region: Optional AWS_DEFAULT_REGION value.
    """
    EOSVC_HOME_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        f"AWS_ACCESS_KEY_ID={access_key_id.strip()}",
        f"AWS_SECRET_ACCESS_KEY={secret_access_key.strip()}",
    ]
    if session_token:
        lines.append(f"AWS_SESSION_TOKEN={session_token.strip()}")
    if region:
        lines.append(f"AWS_REGION={region.strip()}")
    if default_region:
        lines.append(f"AWS_DEFAULT_REGION={default_region.strip()}")
    EOSVC_HOME_ENV.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        os.chmod(EOSVC_HOME_ENV, 0o600)
    except Exception:
        pass
