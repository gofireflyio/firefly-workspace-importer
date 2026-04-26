# Firefly Workspace Importer

Import GitHub repositories that contain Terraform code into [Firefly](https://firefly.ai)
as workspaces and projects. One leaf workspace per Terraform directory; an optional
project tree mirroring your repo structure; idempotent and re-runnable.

- Single-file Python CLI (one runtime dependency: `requests`)
- All configuration is external — no editing the script
- Distroless container image for reproducible runs in CI
- Dry-run mode, retries with backoff, secret masking, incremental result-saving

---

## Contents

- [How it works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Quickstart (native)](#quickstart-native)
- [Quickstart (Docker)](#quickstart-docker)
- [Configuration](#configuration)
  - [Secrets — `.env`](#secrets--env)
  - [Settings — `config.json`](#settings--configjson)
- [Commands](#commands)
- [Output files](#output-files)
- [Idempotency and re-runs](#idempotency-and-re-runs)
- [Troubleshooting](#troubleshooting)

---

## How it works

Two-stage pipeline (run separately or together):

1. **`map`** — Walks each repository on GitHub, finds every directory that
   contains `.tf` files, and writes the structure to
   `github_directory_mapping.json`. Directories without `.tf` files are pruned.
2. **`create`** — Reads the mapping JSON and, for each leaf directory,
   creates a Firefly workspace. If `projects.create` is enabled, mirrors the
   directory tree as a Firefly project hierarchy and attaches workspaces to
   the matching project. Optionally attaches members and variables.

`run` does both end-to-end.

---

## Prerequisites

Either:

- **Python 3.8+** with `pip install -r requirements.txt`, **or**
- **Docker** (recommended for CI / customer onboarding)

Plus:

- A **Firefly account** with API access — generate an Access/Secret key pair at
  *Settings → Access Management → Create Key Pair* (the secret is shown once).
- A configured **VCS integration** in Firefly — note the integration ID at
  *Settings → Integrations*.
- A **GitHub token** for private repos or higher rate limits — generate at
  https://github.com/settings/tokens/new (`repo` scope for private repos).
  Public repos work with no token, capped at 60 requests/hour.

---

## Quickstart (native)

```bash
git clone https://github.com/gofireflyio/firefly-workspace-importer.git
cd firefly-workspace-importer

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env                          # fill in FIREFLY_ACCESS_KEY, FIREFLY_SECRET_KEY, GITHUB_TOKEN
cp config.minimal.example.json config.json    # edit repos and vcs.id (see config.example.json for all options)

# Verify what would happen without touching APIs:
python firefly-workspace-importer.py --dry-run run

# Do it for real:
python firefly-workspace-importer.py run
```

---

## Quickstart (Docker)

```bash
docker build -t firefly-workspace-importer:latest .

docker run --rm \
  --read-only \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  -v "$(pwd):/work" \
  --env-file .env \
  firefly-workspace-importer:latest run
```

The container runs as a non-root user (UID 65532) on a distroless base; only
the mounted `/work` directory is writable. `config.json` and the generated
mapping/results files all live there.

To pin the base images by digest for production builds, see the recipe at the
top of the [Dockerfile](Dockerfile).

---

## Configuration

Configuration is split between **secrets** (`.env`) and **non-secret settings**
(`config.json`). Both default to the current working directory; override with
`--env-file` and `--config`.

### Secrets — `.env`

See [.env.example](.env.example). Environment variables already exported in
the shell take precedence over the file.

| Variable | Required | Description |
| --- | --- | --- |
| `FIREFLY_ACCESS_KEY` | yes | From *Settings → Access Management* |
| `FIREFLY_SECRET_KEY` | yes | Shown once when the key pair is created |
| `FIREFLY_API_URL` | no | Default `https://api.firefly.ai` |
| `GITHUB_TOKEN` | recommended | Required for private repos; use `GH_TOKEN` as a fallback name |

### Settings — `config.json`

Two example files are provided:

- [config.minimal.example.json](config.minimal.example.json) — the smallest
  config that works: just `repos` and `vcs`. Workspaces get created with
  defaults; no projects, members, or variables.
- [config.example.json](config.example.json) — every available knob,
  populated with realistic placeholder values. Copy this when you need
  projects, members, or variables.

Top-level shape:

```jsonc
{
  "repos": ["owner/repo", "owner-or-org"],
  "vcs":       { "id": "...", "type": "github", "default_branch": "main" },
  "workspace": { "runner_type": "...", "iac_type": "terraform", ... },
  "projects":  { "create": true, "main_members": [...], "path_variables": {...} }
}
```

#### `repos` (array of strings)

Each entry is one of:

- `owner/repo` — single repository
- `owner-or-org` — every repository in that organization
- Full GitHub URL of either form

#### `vcs`

| Key | Values | Description |
| --- | --- | --- |
| `id` | string | Firefly VCS integration ID |
| `type` | `github` `gitlab` `bitbucket` `codecommit` `azuredevops` | Must match the integration |
| `default_branch` | string | Default `main` |

#### `workspace`

| Key | Default | Description |
| --- | --- | --- |
| `runner_type` | `firefly` | One of `github-actions`, `gitlab-pipelines`, `bitbucket-pipelines`, `azure-pipelines`, `jenkins`, `semaphore`, `atlantis`, `env0`, `firefly`, `unrecognized` |
| `iac_type` | `terraform` | IaC type |
| `terraform_version` | `1.5.7` | Pinned TF version |
| `execution_triggers` | `["merge"]` | Subset of `merge`, `push`, `pull_request` |
| `apply_rule` | `manual` | `manual` or `auto` |
| `variables` | `[]` | Workspace variables (see schema below) |
| `consumed_variable_sets` | `[]` | Variable set IDs to attach |

**Variable schema** (used in `workspace.variables`, `projects.main_variables`,
and `projects.path_variables`):

```jsonc
{
  "key": "ENV",
  "value": "production",
  "sensitivity": "string",   // "string" or "secret"
  "destination": "env"       // "env" or "iac"
}
```

#### `projects`

| Key | Default | Description |
| --- | --- | --- |
| `create` | `true` | Mirror the directory tree as a project hierarchy |
| `project_id` | `null` | If `create=false`, attach all workspaces to this single project ID |
| `main_members` | `[]` | Members for the per-repo root project (each `{userId, role}`) |
| `main_variables` | `[]` | Variables for the per-repo root project |
| `path_members` | `{}` | Map of `/path/in/repo` → list of members (e.g. `"/aws/production": [...]`) |
| `path_variables` | `{}` | Map of `/path/in/repo` → list of variables |

##### What gets created

Given `repos: ["acme/infra"]` and a repo with this structure:

```
acme/infra/
├── aws/
│   ├── production/main.tf
│   └── staging/main.tf
└── gcp/
    └── dev/main.tf
```

…the importer creates this in Firefly:

```
acme-infra                            ← main project          [main_members + main_variables]
├── acme-infra-aws
│   ├── acme-infra-aws-production     ← path "/aws/production"  [path_members + path_variables]
│   └── acme-infra-aws-staging
└── acme-infra-gcp
    └── acme-infra-gcp-dev

Workspaces:
  acme/infra/aws/production  → attached to acme-infra-aws-production
  acme/infra/aws/staging     → attached to acme-infra-aws-staging
  acme/infra/gcp/dev         → attached to acme-infra-gcp-dev
```

##### `path_*` matching rules

The keys of `path_members` and `path_variables` are matched against the
work-dir paths in the mapping. Mismatches are logged and skipped, not fatal.

| Key | Result |
| --- | --- |
| `/aws/production` | matches the `aws/production` sub-project |
| `/aws` | matches the `aws` mid-level project (vars/members do **not** auto-propagate to children — Firefly's project model decides inheritance) |
| `aws/production` | no match — leading slash required |
| `/AWS/Production` | no match — case sensitive |
| `/aws/prod` | no match — logged as `No project found for path '/aws/prod'` and skipped |

##### When attachments are applied

This is the most common footgun, worth knowing up front:

- **`main_members` and `main_variables`** are applied **only the first time**
  the per-repo root project is created. On subsequent runs, if the project
  already exists, they are skipped (avoids duplicate-add errors). To change
  membership on an existing root project, edit it in the Firefly UI.
- **`path_members` and `path_variables`** are applied on **every run**.
  Firefly's API will decide whether duplicates error or silently update —
  failures are recorded in `firefly_workflows_created.json` but do not
  abort the run.

##### Member schema

```jsonc
{
  "userId": "abc-123",     // get from Firefly /v2/users
  "role": "admin"          // "admin", "member", or "viewer"
}
```

---

## Commands

```text
firefly-workspace-importer.py [--env-file PATH] [--config PATH]
                              [--mapping-file PATH] [--results-file PATH]
                              [--workers N] [--dry-run]
                              [-v|-vv] [-q]
                              {map | create | run}
```

| Subcommand | Effect |
| --- | --- |
| `map` | Scan GitHub → write `github_directory_mapping.json`. No Firefly calls. |
| `create` | Read mapping JSON → create Firefly resources. |
| `run` | `map` + `create`. |

| Flag | Description |
| --- | --- |
| `--env-file` | Path to `.env` file (default `./.env`) |
| `--config` | Path to JSON config (default `./config.json`) |
| `--mapping-file` | Path to mapping JSON (default `./github_directory_mapping.json`) |
| `--results-file` | Path to results JSON (default `./firefly_workflows_created.json`) |
| `--workers N` | Concurrent workspace creations (default `1`, serial) |
| `--dry-run` | Skip every write API call; print what would happen |
| `-v` / `-vv` | Verbosity (info / debug). Default is info. |
| `-q` | Quiet mode (warnings + errors only) |

Exit codes: `0` success · `1` partial failure · `2` config error · `3` auth error.

---

## Output files

- **`github_directory_mapping.json`** — produced by `map`; nested dict of
  directories that contain `.tf` files. Safe to inspect or hand-edit before
  running `create`.
- **`firefly_workflows_created.json`** — produced by `create` / `run`;
  records each workspace creation with success/failure, the matched project
  ID, and totals. Written incrementally so a mid-run crash does not lose
  progress.

---

## Idempotency and re-runs

- The importer queries Firefly's project tree before creating projects and
  reuses any project whose name already exists.
- Workspace names are deterministic (`owner/repo/sub/dir`); re-running after
  partial failure attempts to create only the missing ones (Firefly will
  return an error for duplicates, recorded as a failed entry — review the
  results file before treating as fatal).
- Add new repos to `config.json` and re-run `run` to extend an existing setup.

---

## Troubleshooting

**`Configuration errors:` on startup**
The validator runs before any API call. Read the listed errors — they cite
the exact config path that's wrong.

**`Firefly auth rejected (HTTP 401)`**
Check `FIREFLY_ACCESS_KEY` and `FIREFLY_SECRET_KEY` — the secret is shown
only once at key-pair creation and cannot be recovered. Generate a new pair.

**`branch 'main' not found`**
The repo's default branch isn't `main`. Set `vcs.default_branch` in
`config.json`. (Note: this is a global setting; per-repo overrides are not
yet supported.)

**`GitHub tree for X is truncated`**
The repository tree exceeds GitHub's 100k-entry limit for the recursive
trees endpoint. Either split the repo or extend the importer to walk
sub-trees on demand.

**`No project found for path '/aws/production' (members)`**
A `projects.path_members` (or `path_variables`) key didn't match any
directory in the mapping. Check the mapping file — paths must match exactly
including the leading slash.

---

## Security

See [SECURITY.md](SECURITY.md) for how to report vulnerabilities. Quick
summary of the security posture:

- Secrets only ever loaded from `.env` or environment; never written to logs
  (masked as `abcd...XX`).
- Container image is distroless, runs as non-root (UID 65532), with no shell
  or package manager.
- Recommended `docker run` flags pin `--read-only`, `--cap-drop=ALL`,
  `--security-opt=no-new-privileges`.
- `.gitignore` and `.dockerignore` block `.env`, generated state files, and
  local `config.json` from being committed or baked into images.
