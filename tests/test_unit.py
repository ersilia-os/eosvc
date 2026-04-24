import json
import pytest

from eosvc.constants import (
    EOSVCError,
    BUCKET_PUBLIC,
    BUCKET_PRIVATE,
    BUCKET_MODELS_PUBLIC,
    BUCKET_MODELS_PRIVATE,
    DATA_ROOT,
    OUTPUT_ROOT,
    MODEL_CHECKPOINTS,
    MODEL_FRAMEWORK_FIT,
)
from eosvc.repo import (
    _normalize_access_value,
    AccessPolicy,
    detect_mode,
    load_access,
    ensure_access_lock,
    find_repo_root,
    normalize_user_path,
    category_for_path,
    artifacts_plan,
)


# ---------------------------------------------------------------------------
# _normalize_access_value
# ---------------------------------------------------------------------------

class TestNormalizeAccessValue:
    def test_public_lowercase(self):
        assert _normalize_access_value("public") == "public"

    def test_private_lowercase(self):
        assert _normalize_access_value("private") == "private"

    def test_uppercase(self):
        assert _normalize_access_value("PUBLIC") == "public"
        assert _normalize_access_value("PRIVATE") == "private"

    def test_whitespace_stripped(self):
        assert _normalize_access_value("  public  ") == "public"

    def test_invalid_raises(self):
        with pytest.raises(EOSVCError):
            _normalize_access_value("open")

    def test_empty_raises(self):
        with pytest.raises(EOSVCError):
            _normalize_access_value("")


# ---------------------------------------------------------------------------
# detect_mode
# ---------------------------------------------------------------------------

class TestDetectMode:
    def test_standard_data_key(self):
        assert detect_mode({"data": "public"}) == "standard"

    def test_standard_output_key(self):
        assert detect_mode({"output": "private"}) == "standard"

    def test_standard_both_keys(self):
        assert detect_mode({"data": "public", "output": "public"}) == "standard"

    def test_model_checkpoints_key(self):
        assert detect_mode({"checkpoints": "public"}) == "model"

    def test_model_fit_key(self):
        assert detect_mode({"fit": "public"}) == "model"

    def test_model_both_keys(self):
        assert detect_mode({"checkpoints": "public", "fit": "public"}) == "model"

    def test_mixed_keys_raises(self):
        with pytest.raises(EOSVCError):
            detect_mode({"data": "public", "checkpoints": "public"})

    def test_empty_dict_returns_standard(self):
        # Should not raise — returns "standard" with a warning
        assert detect_mode({}) == "standard"

    def test_none_returns_standard(self):
        assert detect_mode(None) == "standard"


# ---------------------------------------------------------------------------
# AccessPolicy.bucket_for
# ---------------------------------------------------------------------------

class TestAccessPolicyBucketFor:
    def test_data_public(self):
        p = AccessPolicy(data="public", output="public")
        assert p.bucket_for("data") == BUCKET_PUBLIC

    def test_data_private(self):
        p = AccessPolicy(data="private", output="public")
        assert p.bucket_for("data") == BUCKET_PRIVATE

    def test_output_public(self):
        p = AccessPolicy(data="public", output="public")
        assert p.bucket_for("output") == BUCKET_PUBLIC

    def test_output_private(self):
        p = AccessPolicy(data="public", output="private")
        assert p.bucket_for("output") == BUCKET_PRIVATE

    def test_checkpoints_public_uses_models_bucket(self):
        p = AccessPolicy(checkpoints="public", fit="public")
        assert p.bucket_for("checkpoints") == BUCKET_MODELS_PUBLIC

    def test_checkpoints_private_uses_models_bucket(self):
        p = AccessPolicy(checkpoints="private", fit="public")
        assert p.bucket_for("checkpoints") == BUCKET_MODELS_PRIVATE

    def test_fit_public_uses_models_bucket(self):
        p = AccessPolicy(checkpoints="public", fit="public")
        assert p.bucket_for("fit") == BUCKET_MODELS_PUBLIC

    def test_fit_private_uses_models_bucket(self):
        p = AccessPolicy(checkpoints="public", fit="private")
        assert p.bucket_for("fit") == BUCKET_MODELS_PRIVATE

    def test_unknown_category_raises(self):
        p = AccessPolicy(data="public", output="public")
        with pytest.raises(EOSVCError):
            p.bucket_for("unknown")


# ---------------------------------------------------------------------------
# normalize_user_path — standard mode
# ---------------------------------------------------------------------------

class TestNormalizeUserPathStandard:
    def test_data_root(self):
        assert normalize_user_path("data", "standard") == "data"

    def test_data_with_trailing_slash(self):
        assert normalize_user_path("data/", "standard") == "data/"

    def test_data_subpath(self):
        assert normalize_user_path("data/inputs/file.csv", "standard") == "data/inputs/file.csv"

    def test_output_subpath(self):
        assert normalize_user_path("output/results.csv", "standard") == "output/results.csv"

    def test_leading_slash_stripped(self):
        assert normalize_user_path("/data/file.csv", "standard") == "data/file.csv"

    def test_empty_raises(self):
        with pytest.raises(EOSVCError):
            normalize_user_path("", "standard")

    def test_model_path_in_standard_mode_raises(self):
        with pytest.raises(EOSVCError):
            normalize_user_path("model/checkpoints", "standard")

    def test_invalid_root_raises(self):
        with pytest.raises(EOSVCError):
            normalize_user_path("results/file.csv", "standard")


# ---------------------------------------------------------------------------
# normalize_user_path — model mode
# ---------------------------------------------------------------------------

class TestNormalizeUserPathModel:
    def test_full_checkpoints_path(self):
        assert normalize_user_path("model/checkpoints/run1", "model") == "model/checkpoints/run1"

    def test_full_fit_path(self):
        assert normalize_user_path("model/framework/fit/v1", "model") == "model/framework/fit/v1"

    def test_short_checkpoints_expanded(self):
        assert normalize_user_path("checkpoints/run1", "model") == "model/checkpoints/run1"

    def test_short_fit_expanded(self):
        assert normalize_user_path("fit/v1", "model") == "model/framework/fit/v1"

    def test_checkpoints_root_only(self):
        assert normalize_user_path("checkpoints", "model") == "model/checkpoints"

    def test_fit_root_only(self):
        assert normalize_user_path("fit", "model") == "model/framework/fit"

    def test_empty_raises(self):
        with pytest.raises(EOSVCError):
            normalize_user_path("", "model")

    def test_data_path_in_model_mode_raises(self):
        with pytest.raises(EOSVCError):
            normalize_user_path("data/something", "model")

    def test_invalid_model_subpath_raises(self):
        with pytest.raises(EOSVCError):
            normalize_user_path("model/weights", "model")


# ---------------------------------------------------------------------------
# category_for_path
# ---------------------------------------------------------------------------

class TestCategoryForPath:
    def test_data_category(self):
        assert category_for_path("data/inputs/file.csv", "standard") == "data"

    def test_output_category(self):
        assert category_for_path("output/results.csv", "standard") == "output"

    def test_standard_mode_invalid_raises(self):
        with pytest.raises(EOSVCError):
            category_for_path("model/checkpoints", "standard")

    def test_checkpoints_category(self):
        assert category_for_path("model/checkpoints/run1", "model") == "checkpoints"

    def test_fit_category(self):
        assert category_for_path("model/framework/fit/v1", "model") == "fit"

    def test_model_mode_invalid_raises(self):
        with pytest.raises(EOSVCError):
            category_for_path("data/something", "model")


# ---------------------------------------------------------------------------
# artifacts_plan
# ---------------------------------------------------------------------------

class TestArtifactsPlan:
    def test_standard_mode(self):
        plan = artifacts_plan("standard")
        assert plan == [(DATA_ROOT, "data"), (OUTPUT_ROOT, "output")]

    def test_model_mode(self):
        plan = artifacts_plan("model")
        assert plan == [(MODEL_CHECKPOINTS, "checkpoints"), (MODEL_FRAMEWORK_FIT, "fit")]


# ---------------------------------------------------------------------------
# find_repo_root
# ---------------------------------------------------------------------------

class TestFindRepoRoot:
    def test_finds_in_current_dir(self, tmp_path):
        (tmp_path / "access.json").write_text("{}")
        assert find_repo_root(tmp_path) == tmp_path

    def test_finds_in_parent_dir(self, tmp_path):
        (tmp_path / "access.json").write_text("{}")
        subdir = tmp_path / "subdir" / "nested"
        subdir.mkdir(parents=True)
        assert find_repo_root(subdir) == tmp_path

    def test_raises_when_not_found(self, tmp_path):
        with pytest.raises(EOSVCError):
            find_repo_root(tmp_path)


# ---------------------------------------------------------------------------
# load_access
# ---------------------------------------------------------------------------

class TestLoadAccess:
    def test_standard_access_json(self, tmp_path):
        (tmp_path / "access.json").write_text(
            json.dumps({"data": "public", "output": "private"})
        )
        policy, mode = load_access(tmp_path)
        assert mode == "standard"
        assert policy.data == "public"
        assert policy.output == "private"

    def test_model_access_json(self, tmp_path):
        (tmp_path / "access.json").write_text(
            json.dumps({"checkpoints": "public", "fit": "private"})
        )
        policy, mode = load_access(tmp_path)
        assert mode == "model"
        assert policy.checkpoints == "public"
        assert policy.fit == "private"

    def test_missing_access_json_raises(self, tmp_path):
        with pytest.raises(EOSVCError):
            load_access(tmp_path)

    def test_defaults_to_public_when_key_absent(self, tmp_path):
        (tmp_path / "access.json").write_text(json.dumps({"data": "private"}))
        policy, mode = load_access(tmp_path)
        assert policy.output == "public"


# ---------------------------------------------------------------------------
# ensure_access_lock
# ---------------------------------------------------------------------------

class TestEnsureAccessLock:
    def _std_policy(self):
        return AccessPolicy(data="public", output="public")

    def test_creates_lock_on_first_call(self, tmp_path):
        policy = self._std_policy()
        ensure_access_lock(tmp_path, policy, "standard")
        lock = tmp_path / ".eosvc" / "access.lock.json"
        assert lock.exists()
        data = json.loads(lock.read_text())
        assert data["data"] == "public"
        assert data["mode"] == "standard"

    def test_passes_on_matching_policy(self, tmp_path):
        policy = self._std_policy()
        ensure_access_lock(tmp_path, policy, "standard")
        ensure_access_lock(tmp_path, policy, "standard")  # should not raise

    def test_raises_on_policy_change(self, tmp_path):
        ensure_access_lock(tmp_path, self._std_policy(), "standard")
        changed = AccessPolicy(data="private", output="public")
        with pytest.raises(EOSVCError, match="Access policy change detected"):
            ensure_access_lock(tmp_path, changed, "standard")

    def test_raises_on_mode_change(self, tmp_path):
        ensure_access_lock(tmp_path, self._std_policy(), "standard")
        model_policy = AccessPolicy(checkpoints="public", fit="public")
        with pytest.raises(EOSVCError, match="Access policy change detected"):
            ensure_access_lock(tmp_path, model_policy, "model")

    def test_lock_is_written_atomically(self, tmp_path):
        # Verify no .tmp files are left behind after writing
        policy = self._std_policy()
        ensure_access_lock(tmp_path, policy, "standard")
        tmp_files = list((tmp_path / ".eosvc").glob("*.tmp"))
        assert tmp_files == []
