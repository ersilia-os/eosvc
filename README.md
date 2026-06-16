# Ersilia Version Control

EOSVC is a small CLI for syncing large artifacts to **S3**, while your code remains in **Git**. It uploads and downloads with live progress, shows a colored **local ↔ remote diff** (`view`), and can **delete** remote artifacts with explicit, guarded confirmation.

EOSVC supports two repo types (detected from `access.json`):
- **Standard repos**: manage `data/` and `output/`
- **Model repos**: manage `model/checkpoints/` and `model/framework/fit/`

EOSVC **does not** manage Git operations anymore (no clone/pull/push). Use `git` directly for code workflows.

---
## Quick Start
### Installation

Clone the repository and install the package in editable mode:

```bash
git clone https://github.com/ersilia-os/eosvc.git
cd eosvc
pip install -e .
```

Verify that the CLI is available:

```bash
eosvc --help

```
### AWS Credentials Setup

#### 1. Create an AWS Access Key

Create an access key in AWS (IAM) for a user or role with permissions to access the target S3 bucket.

You will need:
- **Access Key ID**
- **Secret Access Key**
- **Session Token** (only if using temporary credentials)
- **AWS Region** (example: `eu-central-2`)

#### 2. Configure EOSVC

Run the following command and replace the placeholders with your credentials:

```bash
eosvc config \
  --access-key-id "..." \
  --secret-access-key "..." \
  --session-token "..." \
  --region "eu-central-2"
```

### Access Rules Configuration (`access.json`)

Create or edit an `access.json` file in your working directory to define which folders are uploaded to S3 and their access level.

### Example

```json
{
  "data": "public",
  "output": "private"
}
```
- Files inside the data/ folder will be uploaded with public access.
- Files inside the output/ folder will be uploaded with private access

#### Git Ignore (Recommended)

If you are working inside a git repository, prevent data folders from being committed by adding them to `.gitignore`:

```gitignore
data/
output/
```

### Upload Data

Upload data from a local directory to S3:
```bash
eosvc upload --path <path-to-data-to-upload>
```

### Download Data

Download data from S3 into a local directory:
```bash
eosvc download --path <path-to-data-to-download>
```

`upload` and `download` show live progress: a Rich progress bar (with size, speed and ETA) when run in a terminal, and one line per file when output is piped or redirected.

### View Differences

Compare your local working tree against S3 and see exactly what is in sync, modified, local-only, or remote-only:
```bash
eosvc view
eosvc view --path data
```

### Delete Remote Data

Remove artifacts from S3 (this only affects the remote copy — your local files are never touched):
```bash
eosvc delete --path <path-to-delete-remotely>
```

## Technical Details

## What EOSVC stores where

EOSVC syncs artifacts under an S3 prefix equal to the **repo name**.

By default, the repo name is the **local folder name** (repo directory basename).  
If your folder name differs from the remote repo/S3 prefix, set:

```bash
export EVC_REPO_NAME="my-actual-repo-name"
````

### Standard repos

Managed roots:

* `data/`
* `output/`

S3 mapping for repo `ersilia-repo`:

* `s3://<bucket>/ersilia-repo/data/...`
* `s3://<bucket>/ersilia-repo/output/...`

### Model repos

Managed roots:

* `model/checkpoints/`
* `model/framework/fit/`

Accepted path aliases for convenience:

* `checkpoints/...` → `model/checkpoints/...`
* `fit/...` → `model/framework/fit/...`

S3 mapping for repo `my-model-repo`:

* `s3://<bucket>/my-model-repo/model/checkpoints/...`
* `s3://<bucket>/my-model-repo/model/framework/fit/...`

In model repos, EOSVC refuses operations on `data/` and `output/`.

---

## Buckets and access

Buckets:

| Bucket | Used by | Access |
|---|---|---|
| `eosvc-public` | Standard repos (`data`, `output`) | Public |
| `eosvc-private` | Standard repos (`data`, `output`) | Private |
| `eosvc-models-public` | Model repos (`checkpoints`, `fit`) | Public |
| `eosvc-models-private` | Model repos (`checkpoints`, `fit`) | Private |

Rules:

* **Read from `eosvc-public` or `eosvc-models-public` may work without AWS credentials** (unsigned S3 client).
* **Read from `eosvc-private` or `eosvc-models-private` requires AWS credentials**.
* **Any upload or delete requires AWS credentials**, regardless of bucket. Deleting also requires `s3:DeleteObject` on the target bucket.

> Note: For unauthenticated reads to work, the public bucket policy must allow `s3:GetObject`.
> For unauthenticated `view` to work, it must also allow `s3:ListBucket` constrained to the relevant prefixes.

---

## Installation

```bash
pip install -e .
```

```bash
eosvc --help
```

---

## Credentials

EOSVC resolves credentials in this order:

1. `.env` files (loaded with `python-dotenv`) from:

   * `<repo>/.config/.env` and `<repo>/.config/eosvc/.env`
   * `./.config/.env` and `./.config/eosvc/.env`
   * `~/.eosvc/.config` (written by `eosvc config`)
   * `<repo>/.env` and `./.env`

2. AWS default credential chain (environment variables and/or `~/.aws/*` if present)
3. Falls back to anonymous — only valid for reads from public buckets

### Option A: environment variables (standard AWS)

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."   # optional
export AWS_REGION="eu-central-2"     # optional
```

### Option B: EOSVC config (writes ~/.eosvc/.config)

EOSVC provides a `config` command to store credentials in:

* `~/.eosvc/.config` (permissions set to `600` when possible)

```bash
eosvc config \
  --access-key-id "..." \
  --secret-access-key "..." \
  --session-token "..." \
  --region "eu-central-2"
```

This is similar in spirit to `aws configure`, but EOSVC writes a `.env` file and loads it alongside other sources.

### Option C: local .env files

Create `.env` in the repo (or current directory):

```bash
AWS_ACCESS_KEY_ID="..."
AWS_SECRET_ACCESS_KEY="..."
AWS_SESSION_TOKEN="..."   # optional
AWS_REGION="eu-central-2"    # optional
```

---

## access.json (required)

EOSVC requires an `access.json` at the repo root.
EOSVC identifies the repo root by searching upward for `access.json` starting from the current directory.

### Standard repo `access.json`

```json
{
  "data": "public",
  "output": "private"
}
```

### Model repo `access.json`

```json
{
  "checkpoints": "public",
  "fit": "public"
}
```

Valid values are: `"public"` or `"private"`.

---

## Commands

### config

Write AWS credentials to `~/.eosvc/.config`:

```bash
eosvc config --access-key-id "..." --secret-access-key "..."
```

Optional flags:

```bash
eosvc config \
  --access-key-id "..." \
  --secret-access-key "..." \
  --session-token "..." \
  --region "eu-central-2" \
  --default-region "eu-central-2"
```

### view (local ↔ remote diff)

`view` compares your **local working tree** against **S3** (by file size) and prints a colored, merged tree+table per managed category, with columns **Path / Status / Local / Remote / Uploaded**:

```bash
eosvc view
eosvc view --path data
eosvc view --path output
eosvc view --path model/checkpoints
eosvc view --path model/framework/fit
eosvc view --path checkpoints
eosvc view --path fit
eosvc view --max-depth 1
```

Each file is classified by status (shown in the legend):

| Status | Meaning |
|---|---|
| `= both (same size)` | present locally and remotely with the same size |
| `~ both (modified)` | present in both, sizes differ |
| `+ local only` | present locally only (would be uploaded) |
| `- remote only` | present in S3 only (would be downloaded) |

Additional cues to make exploration fast:

* a per-category header with a sync dot (**green** = everything in sync, **yellow** = differences), the file count, and total local/remote sizes — with size **units color-graded** by magnitude (`B`→`KB`→`MB`→`GB`→`TB`);
* **per-folder rollups** on directory rows (e.g. `1+ 1- 2~ 3=` plus the folder's byte totals);
* a **grand summary** line across all categories (`Summary: N differ · M same size across K categories`).

Use `--max-depth N` to collapse folders deeper than `N` into a single rollup row for a quick overview.

### download

Download a file or folder from S3 into your repo:

```bash
eosvc download --path data/processed/file.csv
eosvc download --path output/
eosvc download --path model/checkpoints/
eosvc download --path model/framework/fit/
eosvc download --path checkpoints/
eosvc download --path fit/
```

### upload

Upload a file or folder to S3 (requires credentials):

```bash
eosvc upload --path output/some_folder
eosvc upload --path data/test
eosvc upload --path model/checkpoints/test-run
eosvc upload --path model/framework/fit/test-fit
eosvc upload --path checkpoints/test-run
eosvc upload --path fit/test-fit
```

### delete

Delete a file or folder from S3 (**remote only** — your local files are never touched). This is a destructive action, so `delete` is deliberately careful:

```bash
eosvc delete --path data/old_file.csv
eosvc delete --path data
eosvc delete --path .          # all managed artifacts
```

Before anything is removed, `delete`:

1. prints a **red preview** of exactly which remote objects (and total size) will be deleted;
2. shows a **destructive-action warning** — these artifacts are shared, so coordinate with your teammates first, and remember only the remote copy is affected;
3. requires a **typed confirmation** — you type the **path** for a subpath delete, or the **repository name** for `eosvc delete --path .`.

Flags:

* `--yes` — skip the interactive confirmation (the warning is still shown). Required for non-interactive/scripted use; without it, `delete` refuses to run when there is no terminal.
* `--max-depth N` — limit the depth of the preview tree (deeper folders are collapsed to a rollup).

`delete` always requires AWS credentials and `s3:DeleteObject` on the target bucket, regardless of whether the bucket is public or private.

---

## Quick Test

From the project root:

```bash
cd tests
chmod +x test.sh
./test.sh
```

The test script:

* uses `git clone` to obtain test repos
* runs `view`, `upload`, and `download` for both model and standard repos

---

## Access lock (no public/private migration)

EOSVC creates a local lock file:

* `.eosvc/access.lock.json`

If you later change `access.json` (e.g., `public` → `private`), EOSVC will refuse to run.

To override (not recommended), delete the lock file manually:

```bash
rm .eosvc/access.lock.json
```

---

## Common troubleshooting

### “AccessDenied” when reading a public bucket without creds

Your bucket policy probably does not allow anonymous access for the prefixes EOSVC uses.

For **standard repos** (`eosvc-public`), if you want unauthenticated `download` to work:

* allow `s3:GetObject` on `arn:aws:s3:::eosvc-public/*`

If you want unauthenticated `view` to work:

* also allow `s3:ListBucket` on `arn:aws:s3:::eosvc-public`
* restrict with `s3:prefix` conditions for your repo prefixes

For **model repos** (`eosvc-models-public`), apply the same policy to `arn:aws:s3:::eosvc-models-public`.

### “AccessDenied” when deleting

`delete` requires `s3:DeleteObject` on the target bucket. If your principal lacks it, the error includes the credential source and principal so you can fix the IAM policy (or use a different profile).

### “AWS credentials are missing or invalid”

Provide credentials via:

* env vars, or
* `eosvc config` (writes `~/.eosvc/.config`), or
* a `.env` file in the repo/current directory or its `.config/` subdirectory

---

## About the Ersilia Open Source Initiative

The [Ersilia Open Source Initiative](https://ersilia.io) is a tech-nonprofit organization fueling sustainable research in the Global South. Ersilia's main asset is the [Ersilia Model Hub](https://github.com/ersilia-os/ersilia), an open-source repository of AI/ML models for antimicrobial drug discovery.

![Ersilia Logo](assets/Ersilia_Brand.png)
