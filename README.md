# Firefly Workspace Importer

Import repositories that contain Terraform code into [Firefly](https://firefly.ai)
as workspaces and projects, using Firefly's already-configured VCS integration as
the source of truth — **no GitHub/GitLab/Bitbucket credentials required**.

- Single-file Python CLI (one runtime dependency: `requests`)
- All configuration is external — no editing the script
- Distroless container image for reproducible runs in CI
- Interactive review step shows exactly what will be created before any API call
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
- [The review step](#the-review-step)
- [Output files](#output-files)
- [Idempotency and re-runs](#idempotency-and-re-runs)
- [Troubleshooting](#troubleshooting)

---

## How it works

The importer talks **only to Firefly's API**. Repository discovery and
directory scanning go through Firefly's VCS proxy, which uses the credentials
already attached to your VCS integration.

```
1. POST /v2/login                                    → bearer token
2. GET  /v2/api/integrations/global/vcs              → derive vcsType from your integrationId
3. GET  /v2/api/vcs/{type}/{id}/repos?onlyPrivateRepos=true
                                                     → list private repos with default branches
4. GET  /v2/api/vcs/{type}/{id}/directory-tree?...   → file tree per repo (one call per repo)
5. (review tree shown to user; prompt for confirmation)
6. POST /v2/runners/projects                         → create mirroring project hierarchy
7. POST /v2/runners/workspaces                       → one workspace per leaf .tf directory
```

Steps 2–4 happen in `map`. Step 5 onward happens in `create`. `run` does both end-to-end.

---

## Prerequisites

Either:

- **Python 3.8+** with `pip install -r requirements.txt`, **or**
- **Docker** (recommended for CI / customer onboarding)

Plus:

- A **Firefly account** with API access — generate an Access/Secret key pair at
  *Settings → Access Management → Create Key Pair* (the secret is shown once).
- A configured **VCS integration** in Firefly. You only need its **integration
  ID** — the importer derives the type and discovers repositories on its own.
  If you don't know the ID, run `firefly-workspace-importer.py integrations`
  to list every integration accessible to your account.

---

## Quickstart (native)

```bash
git clone https://github.com/gofireflyio/firefly-workspace-importer.git
cd firefly-workspace-importer

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env                          # fill in FIREFLY_ACCESS_KEY and FIREFLY_SECRET_KEY
cp config.minimal.example.json config.json    # set vcs.integrationId

# (Optional) discover your integration ID:
python firefly-workspace-importer.py integrations

# Verify what would happen without touching APIs:
python firefly-workspace-importer.py --dry-run run

# Do it for real (you'll see a review tree and a [y/N] prompt):
python firefly-workspace-importer.py run
```

---

## Quickstart (Docker)

```bash
docker build -t firefly-workspace-importer:latest .

docker run --rm -it \
  --read-only \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  -v "$(pwd):/work" \
  --env-file .env \
  firefly-workspace-importer:latest run
```

The `-it` is needed so the review prompt can read your `[y/N]` answer. In
non-interactive contexts (CI), use `--yes` to skip the prompt:

```bash
docker run --rm -v "$(pwd):/work" --env-file .env \
  firefly-workspace-importer:latest run --yes
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

**No GitHub/GitLab/Bitbucket token is required.** Repo access goes through
Firefly's VCS proxy.

### Settings — `config.json`

Two example files are provided:

- [config.minimal.example.json](config.minimal.example.json) — the smallest
  config that works: just `vcs.integrationId`. Scans every **private** repo
  accessible through that integration; workspaces get created with defaults;
  no projects, members, or variables.
- [config.example.json](config.example.json) — every available knob,
  populated with realistic placeholder values. Copy this when you need
  projects, members, or variables.

Top-level shape:

```jsonc
{
  "vcs":          { "integrationId": "..." },
  "repositories": ["owner/repo1", "owner/repo2"],   // optional; omit = all repos
  "workspace":    { "runner_type": "...", ... },
  "projects":     { "create": true, ... }
}
```

#### `vcs`

| Key | Required | Description |
| --- | --- | --- |
| `integrationId` | yes | Firefly VCS integration ID. The integration's type (`github`, `gitlab`, etc.) and each repo's default branch are derived automatically. |

#### `repositories` (top-level, optional)

A list of `owner/repo` names. **Omit it to scan every private repository
accessible through the integration** (public repos are filtered out — they're
typically forks or upstream contributions, not your IaC). Provide this list
to limit the scan to specific repos.

By default, listing a repo that doesn't exist in the integration is an error
(typo protection). Pass `--ignore-missing-repos` to warn-and-continue instead.

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
| `path_members` | `{}` | Map of `/path/in/repo` → list of members |
| `path_variables` | `{}` | Map of `/path/in/repo` → list of variables |

##### What gets created

Given `repositories: ["acme/infra"]` and a repo with this structure:

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
| `/aws` | matches the `aws` mid-level project |
| `aws/production` | no match — leading slash required |
| `/AWS/Production` | no match — case sensitive |
| `/aws/prod` | no match — logged as `No project found for path '/aws/prod'` and skipped |

##### When attachments are applied

- **`main_members` and `main_variables`** are applied **only the first time**
  the per-repo root project is created. On subsequent runs, if the project
  already exists, they are skipped (avoids duplicate-add errors).
- **`path_members` and `path_variables`** are applied on **every run**.
  Failures are recorded in the results file but do not abort the run.

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
                              [--workers N] [--dry-run] [--yes]
                              [--ignore-missing-repos]
                              [-v|-vv] [-q]
                              {integrations | map | create | run}
```

| Subcommand | Effect |
| --- | --- |
| `integrations` | List Firefly VCS integrations and exit. Useful for finding your `vcs.integrationId`. |
| `map` | Scan via Firefly VCS API → write `firefly_directory_mapping.json`. No write APIs are called. |
| `create` | Read mapping JSON → show review tree → prompt → create Firefly resources. |
| `run` | `map` + `create` end-to-end (one prompt at the create step). |

| Flag | Description |
| --- | --- |
| `--env-file` | Path to `.env` file (default `./.env`) |
| `--config` | Path to JSON config (default `./config.json`) |
| `--mapping-file` | Path to mapping JSON (default `./firefly_directory_mapping.json`) |
| `--results-file` | Path to results JSON (default `./firefly_workflows_created.json`) |
| `--workers N` | Concurrent scan/create operations (default `1`, serial) |
| `--dry-run` | Skip every write API call; print what would happen |
| `--yes`, `-y` | Skip the interactive review prompt before creation |
| `--ignore-missing-repos` | Warn instead of erroring when `repositories` lists names not found in the integration |
| `-v` / `-vv` | Verbosity (info / debug). Default is info. |
| `-q` | Quiet mode (warnings + errors only) |

Exit codes: `0` success · `1` partial failure · `2` config error · `3` auth error · `4` user aborted.

---

## The review step

Before creating anything, `create` (and `run`) prints a tree of every workspace
that will be created and asks you to confirm:

```
======================================================================
Review — workspaces and projects to be created in Firefly
======================================================================

  acme/infra (branch: main)
    ├─ /aws/production
    ├─ /aws/staging
    └─ /gcp/dev

  acme/modules (branch: main)
    └─ /vpc

----------------------------------------------------------------------
Summary
  4 workspace(s)
  9 project(s) (mirroring directory structure, includes 1 main per repo)
======================================================================

Proceed with creation? [y/N]: 
```

- **Interactive run** — you see this prompt.
- **`--yes`** — skip the prompt and proceed (use in CI).
- **`--dry-run`** — show the review, skip the prompt, do nothing.
- **No TTY without `--yes`** — refuses to proceed (safety).

---

## Output files

- **`firefly_directory_mapping.json`** — produced by `map`; per-repo metadata
  plus the nested directory structure (only `.tf`-containing dirs). Inspect or
  hand-edit before running `create`. New format as of the no-credentials
  refactor — old mapping files generated by previous versions need to be
  regenerated.
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
  return an error for duplicates, recorded as a failed entry).
- Add new repos to `config.json` (or remove the `repositories` filter to scan
  everything) and re-run `run` to extend an existing setup.

---

## Troubleshooting

**`Configuration errors:` on startup**
The validator runs before any API call. Read the listed errors — they cite
the exact config path that's wrong.

**`VCS integration <id> not found among N integration(s)`**
Either a typo in `vcs.integrationId`, or the access key doesn't have
permission to see that integration. Run `firefly-workspace-importer.py
integrations` to list what's visible to your credentials.

**`VCS integration ... is disabled or inactive`**
Re-enable it in Firefly (Settings → Integrations) before running the importer.

**`syncStatus=outOfSync` warning**
Firefly hasn't successfully fetched from this integration in a while. The
directory listings used by the importer may be stale. Trigger a fresh fetch
in the Firefly UI, then re-run.

**`Firefly auth rejected (HTTP 401)`**
Check `FIREFLY_ACCESS_KEY` and `FIREFLY_SECRET_KEY` — the secret is shown
only once at key-pair creation and cannot be recovered. Generate a new pair.

**`directory-tree owner/repo@main failed (HTTP 404)`**
The repo isn't accessible through this VCS integration, or the default branch
returned by the `/repos` endpoint doesn't actually exist on the remote.

**`requested repo(s) not found in this integration: owner/typo`**
A name in `repositories` doesn't exist in the integration. Re-run with
`--ignore-missing-repos` to skip it, or fix the typo.

**`No project found for path '/aws/production' (members)`**
A `projects.path_members` (or `path_variables`) key didn't match any
directory in the mapping. Check the mapping file — paths must match exactly
including the leading slash.

---

## Security

See [SECURITY.md](SECURITY.md) for how to report vulnerabilities. Quick
summary of the security posture:

- The only secrets the importer handles are Firefly API credentials. They are
  loaded from `.env` or environment, never written to logs (masked as `abcd...XX`).
- No long-lived VCS tokens (GitHub/GitLab/etc.) ever touch this tool.
- Container image is distroless, runs as non-root (UID 65532), with no shell
  or package manager.
- Recommended `docker run` flags pin `--read-only`, `--cap-drop=ALL`,
  `--security-opt=no-new-privileges`.
- `.gitignore` and `.dockerignore` block `.env`, generated state files, and
  local `config.json` from being committed or baked into images.
