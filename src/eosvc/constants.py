from pathlib import Path


BUCKET_PUBLIC = "eosvc-public"
BUCKET_PRIVATE = "eosvc-private"
BUCKET_MODELS_PUBLIC = "eosvc-models-public"
BUCKET_MODELS_PRIVATE = "eosvc-models-private"

DATA_ROOT = "data"
OUTPUT_ROOT = "output"

MODEL_ROOT = "model"
MODEL_CHECKPOINTS = "model/checkpoints"
MODEL_FRAMEWORK_FIT = "model/fit"

EOSVC_META_DIR = ".eosvc"
ACCESS_LOCK_FILE = "access.lock.json"

EOSVC_HOME_DIR = Path.home() / ".eosvc"
EOSVC_HOME_ENV = EOSVC_HOME_DIR / ".config"


class EOSVCError(RuntimeError):
    """Base exception raised for all eosvc errors."""
    pass
