import json
import os
import tempfile
from pathlib import Path

from eosvc.constants import (
    EOSVCError,
    BUCKET_PUBLIC,
    BUCKET_PRIVATE,
    BUCKET_MODELS_PUBLIC,
    BUCKET_MODELS_PRIVATE,
    DATA_ROOT,
    OUTPUT_ROOT,
    MODEL_ROOT,
    MODEL_CHECKPOINTS,
    MODEL_FRAMEWORK_FIT,
    EOSVC_META_DIR,
    ACCESS_LOCK_FILE,
)
from eosvc.logger import logger


def _read_json(p):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise EOSVCError(f"Failed to parse {p}: {e}") from e


def _normalize_access_value(v):
    v = (v or "").strip().lower()
    if v not in {"public", "private"}:
        raise EOSVCError(f"Invalid access value '{v}'. Use 'public' or 'private'.")
    return v


class AccessPolicy:
    """Describes the public/private access level for each artifact category in a repo.

    For standard repos, `data` and `output` are set.
    For model repos, `checkpoints` and `fit` are set.
    Model categories are routed to eosvc-models-* buckets; standard categories use eosvc-* buckets.
    """

    def __init__(self, data=None, output=None, checkpoints=None, fit=None):
        self.data = _normalize_access_value(data) if data is not None else None
        self.output = _normalize_access_value(output) if output is not None else None
        self.checkpoints = _normalize_access_value(checkpoints) if checkpoints is not None else None
        self.fit = _normalize_access_value(fit) if fit is not None else None

    def bucket_for(self, category):
        """Return the S3 bucket name for the given artifact category.

        Standard categories (data, output) use eosvc-public / eosvc-private.
        Model categories (checkpoints, fit) use eosvc-models-public / eosvc-models-private.

        Raises:
            EOSVCError: If the category is not recognised.
        """
        if category == "data":
            return BUCKET_PUBLIC if self.data == "public" else BUCKET_PRIVATE
        if category == "output":
            return BUCKET_PUBLIC if self.output == "public" else BUCKET_PRIVATE
        if category == "checkpoints":
            return BUCKET_MODELS_PUBLIC if self.checkpoints == "public" else BUCKET_MODELS_PRIVATE
        if category == "fit":
            return BUCKET_MODELS_PUBLIC if self.fit == "public" else BUCKET_MODELS_PRIVATE
        raise EOSVCError(f"Unknown access category: {category}")

    def to_json(self, mode):
        """Serialise the policy to a dict for storage in access.lock.json."""
        if mode == "standard":
            return {"mode": mode, "data": self.data, "output": self.output}
        return {"mode": mode, "checkpoints": self.checkpoints, "fit": self.fit}

    def __eq__(self, other):
        return (
            isinstance(other, AccessPolicy)
            and self.data == other.data
            and self.output == other.output
            and self.checkpoints == other.checkpoints
            and self.fit == other.fit
        )


def detect_mode(d):
    """Detect whether a repo is 'standard' or 'model' from its access.json dict.

    Args:
        d: Parsed access.json dict.

    Returns:
        'model' if checkpoints/fit keys are present, 'standard' otherwise.

    Raises:
        EOSVCError: If both standard and model keys are present in the same file.
    """
    keys = set((d or {}).keys())
    if not keys:
        logger.warning(
            "access.json is empty — defaulting to 'standard' mode. "
            "Add 'data'/'output' or 'checkpoints'/'fit' keys."
        )
    has_std = ("data" in keys) or ("output" in keys)
    has_model = ("checkpoints" in keys) or ("fit" in keys)
    if has_std and has_model:
        raise EOSVCError(
            "access.json cannot mix standard keys (data/output) with model keys (checkpoints/fit)."
        )
    if has_model:
        return "model"
    return "standard"


def require_access_json(repo_dir):
    """Return the path to access.json in repo_dir, raising EOSVCError if it doesn't exist."""
    p = repo_dir / "access.json"
    if not p.exists():
        raise EOSVCError("access.json is required for eosvc operations in this folder.")
    return p


def load_access(repo_dir):
    """Read access.json from repo_dir and return (AccessPolicy, mode).

    Args:
        repo_dir: Path to the repo root containing access.json.

    Returns:
        Tuple of (AccessPolicy, mode_string) where mode_string is 'standard' or 'model'.

    Raises:
        EOSVCError: If access.json is missing or malformed.
    """
    d = _read_json(require_access_json(repo_dir))
    mode = detect_mode(d)
    if mode == "model":
        policy = AccessPolicy(
            checkpoints=d.get("checkpoints", "public"),
            fit=d.get("fit", "public"),
        )
    else:
        policy = AccessPolicy(
            data=d.get("data", "public"),
            output=d.get("output", "public"),
        )
    return policy, mode


def ensure_access_lock(repo_dir, policy, mode):
    """Write or validate the access.lock.json, which freezes the access policy on first use.

    On first call, writes the policy atomically via temp file + rename.
    On subsequent calls, raises EOSVCError if the policy has changed, preventing
    accidental public/private migration.

    Args:
        repo_dir: Path to the repo root.
        policy: The current AccessPolicy.
        mode: The current mode string ('standard' or 'model').

    Raises:
        EOSVCError: If the existing lock differs from the current policy.
    """
    meta_dir = repo_dir / EOSVC_META_DIR
    meta_dir.mkdir(exist_ok=True)
    lock_path = meta_dir / ACCESS_LOCK_FILE

    if not lock_path.exists():
        content = json.dumps(policy.to_json(mode), indent=2) + "\n"
        with tempfile.NamedTemporaryFile(
            "w", dir=meta_dir, delete=False, suffix=".tmp", encoding="utf-8"
        ) as f:
            f.write(content)
            tmp = f.name
        os.replace(tmp, str(lock_path))
        return

    existing = _read_json(lock_path)
    locked_mode = str(existing.get("mode", "standard")).strip().lower() or "standard"
    if locked_mode == "model":
        locked_policy = AccessPolicy(
            checkpoints=existing.get("checkpoints", "public"),
            fit=existing.get("fit", "public"),
        )
    else:
        locked_policy = AccessPolicy(
            data=existing.get("data", "public"),
            output=existing.get("output", "public"),
        )

    if locked_mode != mode or locked_policy != policy:
        raise EOSVCError(
            "Access policy change detected (public/private migration is not allowed).\n"
            f"Lock:   {locked_policy.to_json(locked_mode)}\n"
            f"Config: {policy.to_json(mode)}\n"
            f"If this is intentional, delete {lock_path} manually (NOT recommended)."
        )


def find_repo_root(start_dir):
    """Walk up from start_dir until a directory containing access.json is found.

    Args:
        start_dir: Directory to start searching from.

    Returns:
        Path to the repo root directory.

    Raises:
        EOSVCError: If no access.json is found in start_dir or any parent.
    """
    p = Path(start_dir).resolve()
    for cur in [p] + list(p.parents):
        if (cur / "access.json").exists():
            return cur
    raise EOSVCError("Could not find access.json in this folder or any parent folder.")


def repo_name(repo_dir):
    """Return the repo name, using EVC_REPO_NAME env var if set, otherwise the directory name."""
    v = (os.environ.get("EVC_REPO_NAME") or "").strip()
    return v if v else repo_dir.name


def normalize_user_path(path, mode):
    """Normalise a user-supplied path to its canonical internal form.

    For standard repos, validates the root is 'data/' or 'output/'.
    For model repos, accepts 'model/checkpoints/...', 'model/framework/fit/...', 'checkpoints/...', or
    'fit/...' and normalises all short forms to their full 'model/...' equivalents.

    Args:
        path: Raw path string from the user (e.g. 'checkpoints/run1').
        mode: 'standard' or 'model'.

    Returns:
        Normalised path string.

    Raises:
        EOSVCError: If the path is empty or does not match an allowed root for the mode.
    """
    p = (path or "").strip().lstrip("/")
    if not p:
        raise EOSVCError("--path is required")

    if mode == "standard":
        root = p.split("/", 1)[0]
        if root not in {DATA_ROOT, OUTPUT_ROOT}:
            raise EOSVCError(f"Unsupported path '{p}'. Allowed roots: {DATA_ROOT}/, {OUTPUT_ROOT}/")
        return p

    if p.startswith(MODEL_ROOT + "/"):
        if p.startswith(MODEL_CHECKPOINTS) or p.startswith(MODEL_FRAMEWORK_FIT):
            return p
        if p.startswith("model/fit"):
            rest = p[len("model/fit"):].lstrip("/")
            return f"{MODEL_FRAMEWORK_FIT}/{rest}".rstrip("/")
        raise EOSVCError(
            f"Model repo: only '{MODEL_CHECKPOINTS}/...' or '{MODEL_FRAMEWORK_FIT}/...' are supported."
        )

    if p.startswith("checkpoints"):
        rest = p[len("checkpoints"):].lstrip("/")
        return f"{MODEL_CHECKPOINTS}/{rest}".rstrip("/")

    if p.startswith("fit"):
        rest = p[len("fit"):].lstrip("/")
        return f"{MODEL_FRAMEWORK_FIT}/{rest}".rstrip("/")

    raise EOSVCError(
        "Model repo: only 'model/...', 'checkpoints/...', or 'fit/...' paths are supported."
    )


def category_for_path(path, mode):
    """Map a normalised path to its artifact category.

    Args:
        path: Normalised path string.
        mode: 'standard' or 'model'.

    Returns:
        Category string: 'data', 'output', 'checkpoints', or 'fit'.

    Raises:
        EOSVCError: If the path does not match a known root for the mode.
    """
    p = path.strip().lstrip("/")
    if mode == "standard":
        if p.startswith(DATA_ROOT):
            return "data"
        if p.startswith(OUTPUT_ROOT):
            return "output"
        raise EOSVCError(f"Unsupported path '{p}'.")
    if p.startswith(MODEL_CHECKPOINTS):
        return "checkpoints"
    if p.startswith(MODEL_FRAMEWORK_FIT) or p.startswith("model/fit"):
        return "fit"
    raise EOSVCError(f"Unsupported model path '{p}'.")


def artifacts_plan(mode):
    """Return the list of (local_dir, category) pairs for a bulk upload/download operation.

    Args:
        mode: 'standard' or 'model'.

    Returns:
        List of (relative_dir_string, category_string) tuples.
    """
    if mode == "model":
        return [(MODEL_CHECKPOINTS, "checkpoints"), (MODEL_FRAMEWORK_FIT, "fit")]
    return [(DATA_ROOT, "data"), (OUTPUT_ROOT, "output")]
