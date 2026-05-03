#!/usr/bin/env python3
"""
Firefly Workspace Importer
==========================

End-to-end tool to import repositories that contain Terraform code into
Firefly as workspaces and projects, using Firefly's VCS integrations as
the single source of truth (no GitHub/GitLab/etc. credentials required).

Two-step pipeline (run separately or together):
  1. `map`    - Use the Firefly API to discover repos and `.tf`-containing
                directories under a configured VCS integration; write the
                mapping to a JSON file.
  2. `create` - Read the mapping JSON, show the user a review tree of what
                will be created, prompt for confirmation, then create the
                Firefly workspaces and (optionally) the mirroring project
                hierarchy.

Subcommands:
  integrations - List available Firefly VCS integrations and exit.
  map          - Scan via the Firefly VCS API and write the mapping JSON.
  create       - Read an existing mapping JSON and create Firefly resources.
  run          - map + create in one go.

Configuration:
  Secrets are read from a .env file (default: ./.env).
  Non-secret configuration is read from a JSON file (default: ./config.json).
  See .env.example and config.example.json for templates.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote

import requests

# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------

DEFAULT_FIREFLY_API_URL = "https://api.firefly.ai"
DEFAULT_MAPPING_FILE = "firefly_directory_mapping.json"
DEFAULT_RESULTS_FILE = "firefly_workflows_created.json"
DEFAULT_ENV_FILE = ".env"
DEFAULT_CONFIG_FILE = "config.json"

VCS_TYPE_BUCKETS = ("github", "gitlab", "bitbucket", "bitbucketdc", "codecommit", "azuredevops")
VALID_RUNNER_TYPES = {
    "github-actions", "gitlab-pipelines", "bitbucket-pipelines",
    "azure-pipelines", "jenkins", "semaphore", "atlantis",
    "env0", "firefly", "unrecognized",
}
VALID_TRIGGERS = {"merge", "push", "pull_request"}
VALID_APPLY_RULES = {"manual", "auto"}
VALID_SENSITIVITIES = {"string", "secret"}
VALID_DESTINATIONS = {"env", "iac"}

IAC_FILE_EXTENSIONS = (".tf",)

HTTP_RETRY_STATUSES = {429, 500, 502, 503, 504}
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_BASE = 1.5

EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_CONFIG = 2
EXIT_AUTH = 3
EXIT_USER_ABORT = 4

log = logging.getLogger("firefly")


# ----------------------------------------------------------------------------
# Errors
# ----------------------------------------------------------------------------

class ConfigError(Exception):
    """Raised when configuration is missing or invalid."""


class AuthError(Exception):
    """Raised when authentication with Firefly fails."""


class ApiError(Exception):
    """Raised on non-retryable API failures."""


class UserAbort(Exception):
    """Raised when the user declines the creation prompt."""


# ----------------------------------------------------------------------------
# Config dataclasses
# ----------------------------------------------------------------------------

@dataclass
class Secrets:
    firefly_access_key: str
    firefly_secret_key: str
    firefly_api_url: str = DEFAULT_FIREFLY_API_URL


@dataclass
class VcsConfig:
    integration_id: str


@dataclass
class WorkspaceConfig:
    runner_type: str = "firefly"
    iac_type: str = "opentofu"
    terraform_version: str = "1.11.6"  # also used for opentofu (Firefly API field is `terraformVersion`)
    execution_triggers: list[str] = field(default_factory=lambda: ["merge"])
    apply_rule: str = "manual"
    is_remote: bool = True  # remote state — API field `isRemote`
    variables: list[dict] = field(default_factory=list)
    consumed_variable_sets: list[str] = field(default_factory=list)


@dataclass
class ProjectsConfig:
    create: bool = False  # default: no projects (workspaces only)
    nested: bool = False  # only meaningful when create=True; True = full directory mirror
    project_id: Optional[str] = None
    main_members: list[dict] = field(default_factory=list)
    main_variables: list[dict] = field(default_factory=list)
    path_members: dict[str, list[dict]] = field(default_factory=dict)
    path_variables: dict[str, list[dict]] = field(default_factory=dict)


@dataclass
class Config:
    repositories: list[str]
    vcs: VcsConfig
    workspace: WorkspaceConfig
    projects: ProjectsConfig


@dataclass
class VcsIntegration:
    """Firefly VCS integration metadata, as returned by /integrations/global/vcs."""
    id: str
    name: str
    type: str
    is_enabled: bool
    active: bool
    sync_status: str
    last_fetch_success: Optional[str]


@dataclass
class RepoInfo:
    """Repository metadata as returned by /vcs/{type}/{id}/repos."""
    full_name: str
    default_branch: str
    description: str


# ----------------------------------------------------------------------------
# .env loader (no external dependency)
# ----------------------------------------------------------------------------

def load_env_file(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE .env file. OS environment takes precedence."""
    if not path.exists():
        return {}
    parsed: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        parsed[key] = value
    return parsed


def load_secrets(env_path: Path) -> Secrets:
    file_env = load_env_file(env_path)

    def get(name: str, default: Optional[str] = None) -> Optional[str]:
        return os.environ.get(name) or file_env.get(name) or default

    access = get("FIREFLY_ACCESS_KEY")
    secret = get("FIREFLY_SECRET_KEY")
    api_url = get("FIREFLY_API_URL", DEFAULT_FIREFLY_API_URL) or DEFAULT_FIREFLY_API_URL

    if not access or not secret:
        raise ConfigError(
            f"Missing FIREFLY_ACCESS_KEY or FIREFLY_SECRET_KEY. "
            f"Set them in {env_path} or in the environment."
        )

    return Secrets(
        firefly_access_key=access,
        firefly_secret_key=secret,
        firefly_api_url=api_url.rstrip("/"),
    )


# ----------------------------------------------------------------------------
# Config loader and validation
# ----------------------------------------------------------------------------

def load_config(config_path: Path) -> Config:
    if not config_path.exists():
        raise ConfigError(
            f"Config file not found: {config_path}. "
            f"Copy config.example.json to {config_path.name} and edit it."
        )
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ConfigError(f"Invalid JSON in {config_path}: {e}") from e

    repositories = raw.get("repositories") or []
    vcs = raw.get("vcs") or {}
    ws = raw.get("workspace") or {}
    pr = raw.get("projects") or {}

    integration_id = vcs.get("integrationId") or vcs.get("integration_id") or ""

    return Config(
        repositories=list(repositories),
        vcs=VcsConfig(integration_id=integration_id),
        workspace=WorkspaceConfig(
            runner_type=ws.get("runner_type", "firefly"),
            iac_type=ws.get("iac_type", "opentofu"),
            terraform_version=ws.get("terraform_version", "1.11.6"),
            execution_triggers=list(ws.get("execution_triggers", ["merge"])),
            apply_rule=ws.get("apply_rule", "manual"),
            is_remote=bool(ws.get("is_remote", True)),
            variables=list(ws.get("variables", [])),
            consumed_variable_sets=list(ws.get("consumed_variable_sets", [])),
        ),
        projects=ProjectsConfig(
            create=bool(pr.get("create", False)),
            nested=bool(pr.get("nested", False)),
            project_id=pr.get("project_id"),
            main_members=list(pr.get("main_members", [])),
            main_variables=list(pr.get("main_variables", [])),
            path_members=dict(pr.get("path_members", {})),
            path_variables=dict(pr.get("path_variables", {})),
        ),
    )


def validate_config(cfg: Config) -> None:
    errors: list[str] = []

    if not cfg.vcs.integration_id:
        errors.append("config.vcs.integrationId is required")

    if cfg.workspace.runner_type not in VALID_RUNNER_TYPES:
        errors.append(
            f"config.workspace.runner_type must be one of "
            f"{sorted(VALID_RUNNER_TYPES)}, got {cfg.workspace.runner_type!r}"
        )
    if cfg.workspace.apply_rule not in VALID_APPLY_RULES:
        errors.append(
            f"config.workspace.apply_rule must be one of "
            f"{sorted(VALID_APPLY_RULES)}, got {cfg.workspace.apply_rule!r}"
        )
    bad_triggers = [t for t in cfg.workspace.execution_triggers if t not in VALID_TRIGGERS]
    if bad_triggers:
        errors.append(
            f"config.workspace.execution_triggers contains invalid values "
            f"{bad_triggers}; valid: {sorted(VALID_TRIGGERS)}"
        )
    for label, vars_list in (
        ("workspace.variables", cfg.workspace.variables),
        ("projects.main_variables", cfg.projects.main_variables),
    ):
        errors.extend(_validate_variables(label, vars_list))
    for path, vars_list in cfg.projects.path_variables.items():
        errors.extend(_validate_variables(f"projects.path_variables[{path!r}]", vars_list))
    errors.extend(_validate_members("projects.main_members", cfg.projects.main_members))
    for path, members_list in cfg.projects.path_members.items():
        errors.extend(_validate_members(f"projects.path_members[{path!r}]", members_list))

    if errors:
        raise ConfigError("Configuration errors:\n  - " + "\n  - ".join(errors))

    # Soft warnings — config is valid but a setting won't take effect
    if cfg.projects.create and not cfg.projects.nested:
        if cfg.projects.path_members or cfg.projects.path_variables:
            log.warning(
                "config.projects.path_members and path_variables are set but will "
                "be IGNORED because projects.nested=false (no sub-projects to "
                "attach them to). Set projects.nested=true to enable them."
            )


def _validate_variables(label: str, vars_list: list[dict]) -> list[str]:
    errs: list[str] = []
    for i, v in enumerate(vars_list):
        if not isinstance(v, dict):
            errs.append(f"{label}[{i}] must be an object")
            continue
        if "key" not in v or "value" not in v:
            errs.append(f"{label}[{i}] missing required 'key' or 'value'")
        sens = v.get("sensitivity", "string")
        if sens not in VALID_SENSITIVITIES:
            errs.append(f"{label}[{i}].sensitivity must be one of {sorted(VALID_SENSITIVITIES)}")
        dest = v.get("destination", "env")
        if dest not in VALID_DESTINATIONS:
            errs.append(f"{label}[{i}].destination must be one of {sorted(VALID_DESTINATIONS)}")
    return errs


def _validate_members(label: str, members_list: list[dict]) -> list[str]:
    errs: list[str] = []
    for i, m in enumerate(members_list):
        if not isinstance(m, dict):
            errs.append(f"{label}[{i}] must be an object")
            continue
        if "userId" not in m or "role" not in m:
            errs.append(f"{label}[{i}] missing required 'userId' or 'role'")
    return errs


# ----------------------------------------------------------------------------
# Logging and secret masking
# ----------------------------------------------------------------------------

def mask(value: Optional[str]) -> str:
    if not value:
        return "<unset>"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-2:]}"


def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )


# ----------------------------------------------------------------------------
# HTTP with retries
# ----------------------------------------------------------------------------

def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    max_retries: int = HTTP_MAX_RETRIES,
    **kwargs: Any,
) -> requests.Response:
    """HTTP request with exponential backoff on 429/5xx and connection errors."""
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            resp = session.request(method, url, **kwargs)
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt == max_retries:
                raise
            delay = HTTP_BACKOFF_BASE ** attempt
            log.debug("connection error (attempt %d): %s; retry in %.1fs", attempt + 1, e, delay)
            time.sleep(delay)
            continue

        if resp.status_code in HTTP_RETRY_STATUSES and attempt < max_retries:
            delay = _retry_after(resp) or HTTP_BACKOFF_BASE ** attempt
            log.debug("HTTP %s on %s (attempt %d); retry in %.1fs",
                      resp.status_code, url, attempt + 1, delay)
            time.sleep(delay)
            continue
        return resp
    raise ApiError(f"exhausted retries for {method} {url}: {last_exc}")


def _retry_after(resp: requests.Response) -> Optional[float]:
    val = resp.headers.get("Retry-After")
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


# ----------------------------------------------------------------------------
# Firefly client (Firefly API + VCS proxy endpoints)
# ----------------------------------------------------------------------------

class FireflyClient:
    """Firefly v2 API client. Handles auth, projects, workspaces, and VCS proxy."""

    def __init__(self, secrets: Secrets):
        self.base_url = secrets.firefly_api_url
        self.access_key = secrets.firefly_access_key
        self.secret_key = secrets.firefly_secret_key
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._token: Optional[str] = None

    def login(self) -> None:
        url = f"{self.base_url}/v2/login"
        body = {"accessKey": self.access_key, "secretKey": self.secret_key}
        try:
            resp = request_with_retry(
                self.session, "POST", url,
                data=json.dumps(body),
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            raise AuthError(f"Could not reach Firefly API at {self.base_url}: {e}") from e
        if resp.status_code in (401, 403):
            raise AuthError(
                f"Firefly auth rejected (HTTP {resp.status_code}). "
                f"Check FIREFLY_ACCESS_KEY={mask(self.access_key)} and FIREFLY_SECRET_KEY."
            )
        if not resp.ok:
            raise AuthError(f"Firefly login failed (HTTP {resp.status_code}): {resp.text[:200]}")
        token = resp.json().get("accessToken")
        if not token:
            raise AuthError("Firefly login response did not include accessToken")
        self._token = token
        self.session.headers["Authorization"] = f"Bearer {token}"

    # --- VCS proxy ---

    def list_vcs_integrations(self) -> list[VcsIntegration]:
        """GET /v2/api/integrations/global/vcs — flat list across all type buckets."""
        url = f"{self.base_url}/v2/api/integrations/global/vcs"
        resp = request_with_retry(self.session, "GET", url, timeout=30)
        if not resp.ok:
            raise ApiError(f"list integrations failed (HTTP {resp.status_code}): {resp.text[:200]}")
        data = resp.json()
        out: list[VcsIntegration] = []
        for bucket_name in VCS_TYPE_BUCKETS:
            for item in data.get(bucket_name, []) or []:
                out.append(VcsIntegration(
                    id=item.get("id", ""),
                    name=item.get("name", ""),
                    type=item.get("type", bucket_name),
                    is_enabled=bool(item.get("isEnabled", False)),
                    active=bool(item.get("active", False)),
                    sync_status=item.get("syncStatus", ""),
                    last_fetch_success=item.get("lastFetchSuccess"),
                ))
        return out

    def list_repositories(self, vcs_type: str, integration_id: str) -> list[RepoInfo]:
        """GET /v2/api/vcs/{type}/{id}/repos — private repositories accessible via the integration.

        Restricted to private repos via `onlyPrivateRepos=true` because public repos
        in a customer's integration are typically forks or upstream contributions, not
        the customer's own IaC.
        """
        url = f"{self.base_url}/v2/api/vcs/{vcs_type}/{integration_id}/repos"
        resp = request_with_retry(
            self.session, "GET", url,
            params={"onlyPrivateRepos": "true"},
            timeout=60,
        )
        if not resp.ok:
            raise ApiError(f"list repos failed (HTTP {resp.status_code}): {resp.text[:200]}")
        return [
            RepoInfo(
                full_name=item.get("fullName", ""),
                default_branch=item.get("defaultBranch", ""),
                description=item.get("description", "") or "",
            )
            for item in (resp.json() or [])
        ]

    def get_directory_tree(
        self, vcs_type: str, integration_id: str, repo: str, branch: str,
    ) -> dict[str, Any]:
        """GET /v2/api/vcs/{type}/{id}/directory-tree — recursive file tree for one repo."""
        url = f"{self.base_url}/v2/api/vcs/{vcs_type}/{integration_id}/directory-tree"
        params = {
            "repo": repo,
            "branch": branch,
            "includeFiles": "true",  # required: client-side filters for .tf
        }
        resp = request_with_retry(self.session, "GET", url, params=params, timeout=120)
        if not resp.ok:
            raise ApiError(
                f"directory-tree {repo}@{branch} failed (HTTP {resp.status_code}): "
                f"{resp.text[:200]}"
            )
        return resp.json()

    # --- Projects ---

    def get_projects_tree(self) -> dict[str, Any]:
        url = f"{self.base_url}/v2/runners/projects/tree"
        resp = request_with_retry(self.session, "GET", url, timeout=30)
        if not resp.ok:
            raise ApiError(f"projects/tree failed (HTTP {resp.status_code}): {resp.text[:200]}")
        return resp.json()

    def create_project(
        self, name: str, *, parent_id: Optional[str] = None, description: Optional[str] = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}/v2/runners/projects"
        body: dict[str, Any] = {"name": name}
        if parent_id:
            body["parentId"] = parent_id
        if description:
            body["description"] = description
        return self._post(url, body)

    def add_project_members(self, project_id: str, members: list[dict]) -> dict[str, Any]:
        url = f"{self.base_url}/v2/runners/projects/{project_id}/members"
        return self._post(url, members)

    def add_project_variables(self, project_id: str, variables: list[dict]) -> dict[str, Any]:
        url = f"{self.base_url}/v2/runners/variables/projects/{project_id}/variables"
        return self._post(url, {"variables": variables})

    # --- Workspaces ---

    def create_workspace(self, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/v2/runners/workspaces"
        return self._post(url, body)

    # --- helpers ---

    def _post(self, url: str, body: Any) -> dict[str, Any]:
        resp = request_with_retry(
            self.session, "POST", url, data=json.dumps(body), timeout=60,
        )
        if not resp.ok:
            return {"success": False, "status_code": resp.status_code, "error": resp.text[:500]}
        try:
            data = resp.json() if resp.content else {}
        except ValueError:
            data = {}
        return {"success": True, "status_code": resp.status_code, "data": data}


# ----------------------------------------------------------------------------
# Tree helpers — convert directory-tree response to the importer's mapping
# ----------------------------------------------------------------------------

def tree_to_iac_mapping(tree: dict[str, Any]) -> dict[str, Any]:
    """
    Walk a directory-tree response and return a nested dict containing only
    directories that have at least one IaC file (transitively).

    The response shape is recursive nodes: {name, path, type, children}.
    """
    iac_dirs: set[str] = set()

    def walk(node: dict[str, Any]) -> bool:
        """Return True if this subtree contains any IaC files."""
        if node.get("type") == "file":
            name = node.get("name", "")
            return name.endswith(IAC_FILE_EXTENSIONS) and not name.startswith(".")

        has_iac = False
        for child in node.get("children", []) or []:
            if walk(child):
                has_iac = True
        if has_iac:
            path = node.get("path", "")
            if path and not _path_starts_with_dot(path):
                iac_dirs.add(path)
        return has_iac

    walk(tree)

    mapping: dict[str, Any] = {}
    for path in sorted(iac_dirs):
        cursor = mapping
        for part in path.split("/"):
            cursor = cursor.setdefault(part, {})
    return mapping


def _path_starts_with_dot(path: str) -> bool:
    return any(part.startswith(".") for part in path.split("/"))


def get_leaf_directories(structure: dict[str, Any], base: str = "") -> list[str]:
    """Return paths of leaf directories (no children) from a nested mapping."""
    leaves: list[str] = []
    for name, sub in structure.items():
        path = f"{base}/{name}" if base else name
        if not sub:
            leaves.append(path)
        else:
            leaves.extend(get_leaf_directories(sub, path))
    return leaves


def count_all_directories(structure: dict[str, Any]) -> int:
    """Total number of directory nodes (every level, not just leaves)."""
    count = 0
    for value in structure.values():
        if isinstance(value, dict):
            count += 1
            count += count_all_directories(value)
    return count


def get_all_directory_paths(structure: dict[str, Any], base: str = "") -> list[str]:
    """All directory paths (every level, not just leaves), depth-first."""
    paths: list[str] = []
    for name, sub in structure.items():
        path = f"{base}/{name}" if base else name
        paths.append(path)
        if isinstance(sub, dict) and sub:
            paths.extend(get_all_directory_paths(sub, path))
    return paths


def project_names_for_repo(repo: str, tree: dict[str, Any], *, nested: bool) -> list[str]:
    """Project names the importer would create for this repo (main first)."""
    names = [sanitize_project_name(repo)]
    if nested:
        names.extend(
            sanitize_project_name(workspace_name_for(repo, p))
            for p in get_all_directory_paths(tree)
        )
    return names


def format_work_dir(work_dir: str) -> str:
    return work_dir if work_dir.startswith("/") else f"/{work_dir}"


def sanitize_project_name(name: str) -> str:
    cleaned = name.replace(" ", "-").replace("/", "-")
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")


def workspace_name_for(repo: str, work_dir: str) -> str:
    return f"{repo}/{work_dir.lstrip('/')}"


def find_project_by_name(tree: dict[str, Any], name: str) -> Optional[dict[str, Any]]:
    def walk(items: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        for item in items:
            if item.get("name") == name and "id" in item:
                return item
            if isinstance(item.get("children"), list):
                hit = walk(item["children"])
                if hit:
                    return hit
            if isinstance(item.get("data"), list):
                hit = walk(item["data"])
                if hit:
                    return hit
        return None
    return walk(tree.get("data") or [])


def find_root_projects(tree: dict[str, Any]) -> list[dict[str, Any]]:
    roots: list[dict[str, Any]] = []
    def walk(items: list[dict[str, Any]]) -> None:
        for item in items:
            if "id" in item and not item.get("parentId"):
                roots.append(item)
            if isinstance(item.get("children"), list):
                walk(item["children"])
            if isinstance(item.get("data"), list):
                walk(item["data"])
    walk(tree.get("data") or [])
    return roots


def project_id_for_work_dir(work_dir: str, project_map: dict[str, str]) -> Optional[str]:
    formatted = format_work_dir(work_dir.lstrip("/"))
    if formatted in project_map:
        return project_map[formatted]
    if work_dir in project_map:
        return project_map[work_dir]
    best_id: Optional[str] = None
    best_len = 0
    for path, pid in project_map.items():
        if formatted.startswith(path) and len(path) > best_len:
            best_id, best_len = pid, len(path)
    return best_id


# ----------------------------------------------------------------------------
# Integration resolution and review prompt
# ----------------------------------------------------------------------------

def resolve_integration(
    firefly: FireflyClient, integration_id: str,
) -> VcsIntegration:
    """Look up the integration by ID; raise ConfigError with a helpful list on miss."""
    integrations = firefly.list_vcs_integrations()
    for integ in integrations:
        if integ.id == integration_id:
            if not integ.is_enabled or not integ.active:
                raise ConfigError(
                    f"VCS integration '{integ.name}' ({integ.id}) is disabled or inactive."
                )
            if integ.sync_status and integ.sync_status != "synced":
                log.warning(
                    "VCS integration '%s' has syncStatus=%s (last successful fetch: %s). "
                    "Directory listings may be stale.",
                    integ.name, integ.sync_status, integ.last_fetch_success or "unknown",
                )
            return integ
    raise ConfigError(
        f"VCS integration {integration_id!r} not found among "
        f"{len(integrations)} integration(s). "
        f"Run `integrations` subcommand to see available IDs."
    )


def select_repositories(
    available: list[RepoInfo],
    requested: list[str],
    *,
    ignore_missing: bool,
) -> list[RepoInfo]:
    """Filter `available` to `requested`. Strict by default."""
    if not requested:
        return available

    by_name = {r.full_name: r for r in available}
    selected: list[RepoInfo] = []
    missing: list[str] = []
    for name in requested:
        if name in by_name:
            selected.append(by_name[name])
        else:
            missing.append(name)

    if missing:
        msg = (
            f"{len(missing)} requested repo(s) not found in this integration: "
            f"{', '.join(missing)}"
        )
        if ignore_missing:
            log.warning(msg)
        else:
            raise ConfigError(
                msg + ". Re-run with --ignore-missing-repos to skip them, "
                "or use the `integrations` subcommand to list valid repos."
            )
    return selected


def render_review_tree(
    repo_plans: list[tuple[str, str, list[str], list[str]]],
    *,
    config: Config,
) -> str:
    """
    Render the review tree.

    repo_plans: list of (full_name, default_branch, leaf_dirs, project_names)
    """
    out: list[str] = []
    out.append("=" * 70)
    out.append("Review — workspaces and projects to be created in Firefly")
    out.append("=" * 70)
    out.append("")

    # --- Configuration that will be applied to every workspace ---
    out.append("Workspace configuration:")
    out.append(f"  Runner:              {config.workspace.runner_type}")
    out.append(f"  IaC:                 {config.workspace.iac_type} {config.workspace.terraform_version}")
    out.append(f"  Triggers:            {', '.join(config.workspace.execution_triggers) or '(none)'}")
    out.append(f"  Apply rule:          {config.workspace.apply_rule}")
    out.append(f"  Remote state:        {'on (Firefly-managed)' if config.workspace.is_remote else 'off (workspace manages its own state)'}")

    n_vars = len(config.workspace.variables)
    if n_vars:
        n_secret = sum(1 for v in config.workspace.variables if v.get("sensitivity") == "secret")
        suffix = f" ({n_secret} secret)" if n_secret else ""
        out.append(f"  Workspace variables: {n_vars}{suffix}")
    n_sets = len(config.workspace.consumed_variable_sets)
    if n_sets:
        out.append(f"  Variable sets:       {n_sets}")

    if not config.projects.create:
        mode = "no projects (workspaces unscoped)"
        if config.projects.project_id:
            mode = f"all workspaces attached to project {config.projects.project_id}"
    elif config.projects.nested:
        mode = "full directory tree mirror"
    else:
        mode = "one main project per repo"
    out.append(f"  Project mode:        {mode}")

    if config.projects.create:
        n_mm = len(config.projects.main_members)
        n_mv = len(config.projects.main_variables)
        if n_mm or n_mv:
            parts = []
            if n_mm:
                parts.append(f"{n_mm} member(s)")
            if n_mv:
                parts.append(f"{n_mv} variable(s)")
            out.append(f"  Main project attach: {', '.join(parts)}")
        if config.projects.nested:
            n_pm = len(config.projects.path_members)
            n_pv = len(config.projects.path_variables)
            if n_pm or n_pv:
                out.append(f"  Path-specific:       "
                           f"{n_pm} path(s) with members, {n_pv} path(s) with variables")
    out.append("")
    out.append("-" * 70)
    out.append("Repositories:")
    out.append("")

    total_workspaces = 0
    total_projects = 0
    for full_name, branch, leaves, project_names in repo_plans:
        if not leaves:
            out.append(f"  {full_name} (branch: {branch})")
            out.append(f"    (no Terraform directories found — will be skipped)")
            out.append("")
            continue
        out.append(f"  {full_name} (branch: {branch})")
        for i, leaf in enumerate(leaves):
            connector = "└─" if i == len(leaves) - 1 else "├─"
            out.append(f"    {connector} {format_work_dir(leaf)}")
        out.append("")
        total_workspaces += len(leaves)
        total_projects += len(project_names)

    out.append("-" * 70)
    out.append("Summary")
    out.append(f"  {total_workspaces} workspace(s)")
    if config.projects.create:
        if config.projects.nested:
            out.append(f"  {total_projects} project(s) — main + sub-project per directory level")
        else:
            out.append(f"  {total_projects} project(s) — one main project per repo")
    else:
        out.append("  No projects will be created (projects.create=false)")

    if config.projects.create and total_projects > 0:
        out.append("")
        out.append("  Projects to create:")
        for _, _, leaves, project_names in repo_plans:
            if not leaves:  # repos with no leaves are skipped → no projects either
                continue
            for name in project_names:
                out.append(f"    New --> {name}")

    if total_workspaces > 0:
        out.append("")
        out.append("  Workspaces to create:")
        for full_name, _, leaves, _ in repo_plans:
            for leaf in leaves:
                out.append(f"    New --> {workspace_name_for(full_name, leaf)}")
    out.append("=" * 70)
    return "\n".join(out)


def confirm_creation(*, dry_run: bool, yes: bool) -> bool:
    """Prompt the user to confirm. Returns False if declined or non-TTY without --yes.

    Writes prompts to stderr so output ordering survives stdout redirection.
    """
    if dry_run:
        print("\nDry-run mode — skipping confirmation prompt; nothing will be created.",
              file=sys.stderr)
        return True
    if yes:
        print("\n--yes provided — proceeding without confirmation.", file=sys.stderr)
        return True
    if not sys.stdin.isatty():
        print(
            "\nERROR: stdin is not a TTY and --yes was not provided.\n"
            "       Re-run with --yes to bypass the confirmation prompt.",
            file=sys.stderr,
        )
        return False
    try:
        sys.stderr.write("\nProceed with creation? [y/N]: ")
        sys.stderr.flush()
        answer = input().strip().lower()
    except (EOFError, KeyboardInterrupt):
        sys.stderr.write("\n")
        return False
    return answer in ("y", "yes")


# ----------------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------------

@dataclass
class Results:
    total_repos: int = 0
    total_repos_skipped: int = 0  # skipped due to unrecoverable project conflict
    total_workflows_created: int = 0
    total_workflows_failed: int = 0
    total_workflows_skipped: int = 0  # already-exists, treated as idempotent success
    total_projects_created: int = 0
    total_projects_failed: int = 0
    workflows: list[dict] = field(default_factory=list)
    projects: list[dict] = field(default_factory=list)


class Orchestrator:
    def __init__(
        self,
        firefly: FireflyClient,
        config: Config,
        *,
        dry_run: bool,
        results_path: Path,
        workers: int,
    ):
        self.firefly = firefly
        self.config = config
        self.dry_run = dry_run
        self.results_path = results_path
        self.workers = max(1, workers)

    # --- map ---

    def build_mapping(self, *, ignore_missing_repos: bool) -> dict[str, Any]:
        """Discover repos and Terraform directories via the Firefly VCS API."""
        integration = resolve_integration(self.firefly, self.config.vcs.integration_id)
        log.info("Using VCS integration '%s' (%s, %s)",
                 integration.name, integration.id, integration.type)

        log.info("Listing repositories under integration...")
        all_repos = self.firefly.list_repositories(integration.type, integration.id)
        log.info("  %d repo(s) accessible via this integration", len(all_repos))

        repos = select_repositories(
            all_repos, self.config.repositories, ignore_missing=ignore_missing_repos,
        )
        if self.config.repositories:
            log.info("  filtered to %d requested repo(s)", len(repos))

        if self.workers == 1 or len(repos) == 1:
            scan_results = [self._scan_repo(integration, r) for r in repos]
        else:
            log.info("  scanning with %d worker(s) in parallel", self.workers)
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = {ex.submit(self._scan_repo, integration, r): r for r in repos}
                scan_results = [f.result() for f in as_completed(futures)]

        # Stable order by repo full name
        scan_results.sort(key=lambda x: x[0])

        mapping: dict[str, Any] = {}
        for full_name, repo_meta in scan_results:
            mapping[full_name] = repo_meta
        return mapping

    def _scan_repo(
        self, integration: VcsIntegration, repo: RepoInfo,
    ) -> tuple[str, dict[str, Any]]:
        log.info("Scanning %s @ %s", repo.full_name, repo.default_branch)
        try:
            tree = self.firefly.get_directory_tree(
                integration.type, integration.id, repo.full_name, repo.default_branch,
            )
            iac_tree = tree_to_iac_mapping(tree)
            leaves = len(get_leaf_directories(iac_tree))
            log.info("  %s: %d Terraform leaf dir(s)", repo.full_name, leaves)
            return repo.full_name, {
                "vcsType": integration.type,
                "defaultBranch": repo.default_branch,
                "description": repo.description,
                "tree": iac_tree,
            }
        except ApiError as e:
            log.error("  %s: scan failed: %s", repo.full_name, e)
            return repo.full_name, {
                "vcsType": integration.type,
                "defaultBranch": repo.default_branch,
                "description": repo.description,
                "tree": {},
                "error": str(e),
            }

    # --- create ---

    def create_all(self, mapping: dict[str, Any], *, yes: bool) -> Results:
        results = Results(total_repos=len(mapping))

        repo_plans: list[tuple[str, str, list[str], list[str]]] = []
        for repo, meta in mapping.items():
            if not isinstance(meta, dict) or "tree" not in meta:
                raise ConfigError(
                    f"Mapping entry for {repo!r} is in an old/unknown format. "
                    f"Re-run `map` to regenerate {self.results_path.name}'s sibling mapping file."
                )
            branch = meta.get("defaultBranch", "")
            tree = meta.get("tree") or {}
            leaves = get_leaf_directories(tree)
            if self.config.projects.create:
                projects = project_names_for_repo(repo, tree, nested=self.config.projects.nested)
            else:
                projects = []
            repo_plans.append((repo, branch, leaves, projects))

        # Always print the review tree (stderr for stable interleaving with logs)
        print(file=sys.stderr)
        print(render_review_tree(repo_plans, config=self.config), file=sys.stderr)

        if not confirm_creation(dry_run=self.dry_run, yes=yes):
            raise UserAbort("User declined the creation prompt")

        for repo, branch, leaves, _ in repo_plans:
            meta = mapping[repo]
            if "error" in meta:
                log.warning("Skipping %s: scan error (%s)", repo, meta["error"])
                results.total_repos_skipped += 1
                continue
            if not leaves:
                log.info("Skipping %s: no Terraform directories found", repo)
                results.total_repos_skipped += 1
                continue
            vcs_type = meta.get("vcsType")
            if not vcs_type:
                raise ConfigError(
                    f"Mapping entry for {repo!r} is missing 'vcsType'. "
                    f"This means the mapping file was produced by an older version "
                    f"of the importer. Re-run `map` (or `run`) to regenerate it."
                )
            log.info("Repository: %s", repo)

            tree = meta["tree"]
            project_map: dict[str, str] = {}
            root_project_id: Optional[str] = None
            if self.config.projects.create:
                root_project_id, project_map, created_count, failed_count, skip_repo = \
                    self._build_projects_for_repo(repo, tree)
                results.total_projects_created += created_count
                results.total_projects_failed += failed_count
                if skip_repo:
                    log.warning(
                        "  Skipping all workspaces for %s — main project conflict is "
                        "unrecoverable. See instructions above.", repo,
                    )
                    results.total_repos_skipped += 1
                    self._save_results(results)
                    continue
                if self.config.projects.nested:
                    self._apply_path_attachments(project_map)

            self._create_workspaces_for_repo(
                repo, vcs_type, branch, leaves, project_map, root_project_id, results,
            )

            if self.config.projects.create:
                results.projects.append({
                    "repo": repo,
                    "root_project_id": root_project_id,
                    "project_map": project_map,
                })

            self._save_results(results)

        return results

    def _build_projects_for_repo(
        self, repo: str, structure: dict[str, Any],
    ) -> tuple[Optional[str], dict[str, str], int, int, bool]:
        """Returns (root_project_id, project_map, created_count, failed_count, skip_repo).

        skip_repo=True signals the caller to skip workspace creation for this repo
        because main-project setup did not complete (and orphan workspaces are worse
        than no workspaces).
        """
        created = 0
        failed = 0
        if self.dry_run:
            log.info("  [dry-run] would build project tree for %s", repo)
            return f"<dry-run:{sanitize_project_name(repo)}>", {}, 0, 0, False

        existing_tree = self.firefly.get_projects_tree()
        existing_root = find_root_projects(existing_tree)
        existing_root_id = existing_root[0]["id"] if existing_root else None

        repo_project_name = sanitize_project_name(repo)
        existing_main = find_project_by_name(existing_tree, repo_project_name)
        if existing_main:
            root_project_id = existing_main["id"]
            log.info("  Main project exists: %s (%s)", repo_project_name, root_project_id)
        else:
            log.info("  Creating main project: %s", repo_project_name)
            res = self.firefly.create_project(
                name=repo_project_name,
                parent_id=existing_root_id,
                description=f"Main project for {repo}",
            )
            if not res["success"]:
                log.error("    Failed: %s", res.get("error"))
                failed += 1
                if _looks_like_already_exists(res):
                    log.error(
                        "    Project '%s' exists in Firefly (likely from a previous "
                        "run) but cannot be located via the API for re-use.",
                        repo_project_name,
                    )
                    log.error(
                        "    To re-run cleanly: delete this project in the Firefly "
                        "Projects UI, then re-run the importer.",
                    )
                return None, {}, 0, failed, True

            root_project_id = res["data"].get("id")
            created += 1
            self._apply_main_attachments(root_project_id)

        project_map: dict[str, str] = {}
        if self.config.projects.nested:
            sub_created, sub_failed = self._build_subtree(
                structure, repo, "", root_project_id, existing_tree, project_map,
            )
            created += sub_created
            failed += sub_failed
        return root_project_id, project_map, created, failed, False

    def _build_subtree(
        self,
        structure: dict[str, Any],
        repo: str,
        base_path: str,
        parent_id: Optional[str],
        existing_tree: dict[str, Any],
        project_map: dict[str, str],
    ) -> tuple[int, int]:
        """Returns (created_count, failed_count)."""
        created = 0
        failed = 0
        for dir_name, subdirs in structure.items():
            current = f"{base_path}/{dir_name}" if base_path else dir_name
            formatted = format_work_dir(current)
            project_name = sanitize_project_name(workspace_name_for(repo, current))

            existing = find_project_by_name(existing_tree, project_name)
            if existing:
                project_id = existing["id"]
                project_map[formatted] = project_id
                log.debug("    Project exists: %s (%s)", project_name, project_id)
            else:
                log.info("    Creating project: %s", project_name)
                res = self.firefly.create_project(
                    name=project_name,
                    parent_id=parent_id,
                    description=f"Project for {repo}{formatted}",
                )
                if not res["success"]:
                    log.error("      Failed: %s", res.get("error"))
                    failed += 1
                    continue
                project_id = res["data"].get("id")
                project_map[formatted] = project_id
                created += 1

            if subdirs and project_id:
                sub_created, sub_failed = self._build_subtree(
                    subdirs, repo, current, project_id, existing_tree, project_map,
                )
                created += sub_created
                failed += sub_failed
        return created, failed

    def _apply_main_attachments(self, project_id: Optional[str]) -> None:
        if not project_id:
            return
        if self.config.projects.main_members:
            log.info("    Adding %d member(s) to main project", len(self.config.projects.main_members))
            res = self.firefly.add_project_members(project_id, self.config.projects.main_members)
            if not res["success"]:
                log.error("      Failed: %s", res.get("error"))
        if self.config.projects.main_variables:
            log.info("    Adding %d variable(s) to main project", len(self.config.projects.main_variables))
            res = self.firefly.add_project_variables(project_id, self.config.projects.main_variables)
            if not res["success"]:
                log.error("      Failed: %s", res.get("error"))

    def _apply_path_attachments(self, project_map: dict[str, str]) -> None:
        for path, members in self.config.projects.path_members.items():
            normalized = format_work_dir(path.lstrip("/"))
            pid = project_map.get(normalized) or project_map.get(path)
            if not pid:
                log.warning("  No project found for path %s (members)", normalized)
                continue
            if self.dry_run:
                log.info("  [dry-run] would add %d member(s) to %s", len(members), normalized)
                continue
            log.info("  Adding %d member(s) to %s", len(members), normalized)
            res = self.firefly.add_project_members(pid, members)
            if not res["success"]:
                log.error("    Failed: %s", res.get("error"))

        for path, variables in self.config.projects.path_variables.items():
            normalized = format_work_dir(path.lstrip("/"))
            pid = project_map.get(normalized) or project_map.get(path)
            if not pid:
                log.warning("  No project found for path %s (variables)", normalized)
                continue
            if self.dry_run:
                log.info("  [dry-run] would add %d variable(s) to %s", len(variables), normalized)
                continue
            log.info("  Adding %d variable(s) to %s", len(variables), normalized)
            res = self.firefly.add_project_variables(pid, variables)
            if not res["success"]:
                log.error("    Failed: %s", res.get("error"))

    def _create_workspaces_for_repo(
        self,
        repo: str,
        vcs_type: str,
        default_branch: str,
        leaf_dirs: list[str],
        project_map: dict[str, str],
        root_project_id: Optional[str],
        results: Results,
    ) -> None:
        bodies: list[tuple[str, str, dict]] = []
        for work_dir in leaf_dirs:
            formatted = format_work_dir(work_dir)
            ws_name = workspace_name_for(repo, work_dir)
            # Lookup chain (first non-None wins):
            #   1. nested sub-project matching this work_dir (if projects.nested=True)
            #   2. main project for this repo (when projects.create=True)
            #   3. global projects.project_id (when projects.create=False)
            if self.config.projects.create:
                project_id = (
                    project_id_for_work_dir(formatted, project_map)
                    or root_project_id
                )
            else:
                project_id = self.config.projects.project_id

            body: dict[str, Any] = {
                "runnerType": self.config.workspace.runner_type,
                "iacType": self.config.workspace.iac_type,
                "workspaceName": ws_name,
                "vcsId": self.config.vcs.integration_id,
                "vcsType": vcs_type,
                "repo": repo,
                "defaultBranch": default_branch,
                "workDir": formatted,
                "isRemote": self.config.workspace.is_remote,
                "variables": self.config.workspace.variables,
                "execution": {
                    "triggers": self.config.workspace.execution_triggers,
                    "applyRule": self.config.workspace.apply_rule,
                    "terraformVersion": self.config.workspace.terraform_version,
                },
                "description": f"Workflow for {repo}{formatted}",
                "project": project_id,
            }
            if self.config.workspace.consumed_variable_sets:
                body["consumedVariableSets"] = self.config.workspace.consumed_variable_sets
            bodies.append((ws_name, formatted, body))

        if self.dry_run:
            for ws_name, formatted, _ in bodies:
                log.info("  [dry-run] would create workspace %s (workDir=%s)", ws_name, formatted)
            return

        if self.workers == 1:
            for ws_name, formatted, body in bodies:
                log.info("  Creating workspace: %s", ws_name)
                res = self.firefly.create_workspace(body)
                self._record_workspace(repo, ws_name, formatted, body, res, results)
                self._save_results(results)
            return

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {
                ex.submit(self.firefly.create_workspace, body): (ws_name, formatted, body)
                for ws_name, formatted, body in bodies
            }
            for fut in as_completed(futures):
                ws_name, formatted, body = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:  # noqa: BLE001
                    res = {"success": False, "error": str(e)}
                self._record_workspace(repo, ws_name, formatted, body, res, results)
                self._save_results(results)

    def _record_workspace(
        self,
        repo: str,
        ws_name: str,
        formatted: str,
        body: dict,
        res: dict,
        results: Results,
    ) -> None:
        info: dict[str, Any] = {
            "repo": repo,
            "work_dir": formatted,
            "workspace_name": ws_name,
            "success": res["success"],
        }
        if body.get("project"):
            info["project_id"] = body["project"]
        if res["success"]:
            log.info("    OK %s", ws_name)
            info["workspace_id"] = (res.get("data") or {}).get("id")
            info["status_code"] = res.get("status_code")
            results.total_workflows_created += 1
        elif _looks_like_workspace_already_exists(res):
            log.info("    SKIP %s (already exists)", ws_name)
            info["status"] = "already_exists"
            info["status_code"] = res.get("status_code")
            info["success"] = True  # idempotent — treat as success in the results file
            results.total_workflows_skipped += 1
        else:
            log.error("    FAIL %s: %s", ws_name, res.get("error"))
            info["error"] = res.get("error")
            info["status_code"] = res.get("status_code")
            results.total_workflows_failed += 1
        results.workflows.append(info)

    def _save_results(self, results: Results) -> None:
        if self.dry_run:
            return
        payload = {
            "total_repos": results.total_repos,
            "total_repos_skipped": results.total_repos_skipped,
            "total_workflows_created": results.total_workflows_created,
            "total_workflows_failed": results.total_workflows_failed,
            "total_workflows_skipped": results.total_workflows_skipped,
            "total_projects_created": results.total_projects_created,
            "total_projects_failed": results.total_projects_failed,
            "workflows": results.workflows,
            "projects": results.projects,
        }
        tmp = self.results_path.with_suffix(self.results_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.results_path)


def _looks_like_already_exists(res: dict) -> bool:
    """Detect 'project already exists' from a create_project failure response."""
    if res.get("status_code") != 409:
        return False
    err = (res.get("error") or "").lower()
    return "already exists" in err


def _looks_like_workspace_already_exists(res: dict) -> bool:
    """Detect 'workspace already exists' from a create_workspace failure response."""
    if res.get("status_code") != 409:
        return False
    err = (res.get("error") or "").lower()
    return "workspace already exists" in err or "already exists" in err


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

KNOWN_SUBCOMMANDS = {"integrations", "map", "create", "run"}
VALUE_FLAGS = {"--env-file", "--config", "--mapping-file", "--results-file", "--workers"}


def _reorder_argv_for_subcommand(argv: list[str]) -> list[str]:
    """Move global flags from before the subcommand to AFTER it.

    Argparse's `parents=` mechanism has a known footgun: when the same flag is
    defined on both the main parser and a subparser, the subparser's default
    silently overwrites the main parser's value. So `--dry-run create` would
    parse but produce `dry_run=False`. To dodge this we shift any flags that
    appear before the subcommand to after it, so they're always parsed by the
    subparser instance that wins.
    """
    sub_idx = None
    skip_next = False
    for i, arg in enumerate(argv):
        if skip_next:
            skip_next = False
            continue
        # `--flag=value` is one element; no separate value to skip.
        if arg.startswith("--") and "=" in arg:
            continue
        if arg in KNOWN_SUBCOMMANDS:
            sub_idx = i
            break
        if arg in VALUE_FLAGS:
            skip_next = True
    if sub_idx is None or sub_idx == 0:
        return argv
    return [argv[sub_idx]] + argv[:sub_idx] + argv[sub_idx + 1:]


def build_parser() -> argparse.ArgumentParser:
    # Common flags live on a parent parser so they work whether passed BEFORE
    # the subcommand (`cmd --dry-run run`) or AFTER it (`cmd run --dry-run`).
    # Argv is reordered upstream so flags always parse on the subparser side.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--env-file", default=DEFAULT_ENV_FILE,
                        help=f"Path to .env file with secrets (default: {DEFAULT_ENV_FILE})")
    common.add_argument("--config", default=DEFAULT_CONFIG_FILE,
                        help=f"Path to JSON config file (default: {DEFAULT_CONFIG_FILE})")
    common.add_argument("--mapping-file", default=DEFAULT_MAPPING_FILE,
                        help=f"Path to mapping JSON (default: {DEFAULT_MAPPING_FILE})")
    common.add_argument("--results-file", default=DEFAULT_RESULTS_FILE,
                        help=f"Path to results JSON (default: {DEFAULT_RESULTS_FILE})")
    common.add_argument("--workers", type=int, default=1,
                        help="Concurrent scan/create operations (default: 1, serial)")
    common.add_argument("--dry-run", action="store_true",
                        help="Do not call write APIs; show what would happen")
    common.add_argument("--yes", "-y", action="store_true",
                        help="Skip the interactive review prompt before creation")
    common.add_argument("--ignore-missing-repos", action="store_true",
                        help="Warn instead of erroring when config.repositories lists "
                             "names not found in the integration")
    common.add_argument("-v", "--verbose", action="count", default=1,
                        help="Increase verbosity (-v info [default], -vv debug)")
    common.add_argument("-q", "--quiet", action="store_true",
                        help="Only warnings and errors")

    p = argparse.ArgumentParser(
        prog="firefly-workspace-importer.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[common],
    )

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("integrations", parents=[common],
                   help="List Firefly VCS integrations and exit")
    sub.add_parser("map", parents=[common],
                   help="Scan via Firefly VCS API and write mapping JSON")
    sub.add_parser("create", parents=[common],
                   help="Read mapping JSON and create Firefly resources")
    sub.add_parser("run", parents=[common],
                   help="map + create end-to-end")
    return p


def cmd_integrations(firefly: FireflyClient) -> int:
    integrations = firefly.list_vcs_integrations()
    if not integrations:
        print("No VCS integrations configured in Firefly.")
        return EXIT_OK
    print(f"{'ID':<26}  {'TYPE':<12}  {'NAME':<40}  {'SYNC':<10}  LAST FETCH SUCCESS")
    for integ in integrations:
        marker = " " if integ.is_enabled and integ.active else "*"
        last = (integ.last_fetch_success or "").replace("T", " ").split(".")[0]
        print(f"{integ.id:<26}  {integ.type:<12}  {integ.name[:40]:<40}  "
              f"{integ.sync_status:<10}  {last}{marker}")
    print("\n* = disabled or inactive")
    return EXIT_OK


def main(argv: Optional[Iterable[str]] = None) -> int:
    raw = list(argv) if argv is not None else sys.argv[1:]
    args = build_parser().parse_args(_reorder_argv_for_subcommand(raw))
    setup_logging(0 if args.quiet else args.verbose)

    env_path = Path(args.env_file)
    config_path = Path(args.config)
    mapping_path = Path(args.mapping_file)
    results_path = Path(args.results_file)

    try:
        secrets = load_secrets(env_path)
    except ConfigError as e:
        log.error("%s", e)
        return EXIT_CONFIG

    log.info("Firefly API: %s", secrets.firefly_api_url)
    log.info("Firefly access key: %s", mask(secrets.firefly_access_key))
    if args.dry_run:
        log.warning("DRY-RUN: no write APIs will be called")

    try:
        # `integrations` is a quick read-only command — no config needed beyond secrets
        if args.command == "integrations":
            firefly = FireflyClient(secrets)
            firefly.login()
            return cmd_integrations(firefly)

        config = load_config(config_path)
        validate_config(config)

        firefly = FireflyClient(secrets)
        # `create --dry-run` only reads the local mapping and prints a plan,
        # so we can skip auth. `map` and `run` need real API access either way.
        needs_login = not (args.dry_run and args.command == "create")
        if needs_login:
            firefly.login()
        orch = Orchestrator(
            firefly, config,
            dry_run=args.dry_run, results_path=results_path, workers=args.workers,
        )

        if args.command == "map":
            mapping = orch.build_mapping(ignore_missing_repos=args.ignore_missing_repos)
            mapping_path.write_text(
                json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8",
            )
            log.warning("Wrote mapping to %s", mapping_path)
            return EXIT_OK

        if args.command == "create":
            if not mapping_path.exists():
                log.error("Mapping file not found: %s. Run `map` first.", mapping_path)
                return EXIT_CONFIG
            mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
            results = orch.create_all(mapping, yes=args.yes)
            _print_summary(results)
            had_failures = (
                results.total_workflows_failed > 0
                or results.total_projects_failed > 0
                or results.total_repos_skipped > 0
            )
            return EXIT_PARTIAL if had_failures else EXIT_OK

        if args.command == "run":
            mapping = orch.build_mapping(ignore_missing_repos=args.ignore_missing_repos)
            mapping_path.write_text(
                json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8",
            )
            log.warning("Wrote mapping to %s", mapping_path)
            results = orch.create_all(mapping, yes=args.yes)
            _print_summary(results)
            had_failures = (
                results.total_workflows_failed > 0
                or results.total_projects_failed > 0
                or results.total_repos_skipped > 0
            )
            return EXIT_PARTIAL if had_failures else EXIT_OK

    except ConfigError as e:
        log.error("%s", e)
        return EXIT_CONFIG
    except AuthError as e:
        log.error("%s", e)
        return EXIT_AUTH
    except UserAbort as e:
        log.warning("Aborted: %s", e)
        return EXIT_USER_ABORT
    except KeyboardInterrupt:
        log.error("Interrupted")
        return EXIT_USER_ABORT

    return EXIT_OK


def _print_summary(results: Results) -> None:
    log.warning(
        "Summary: repos=%d skipped=%d  workspaces created=%d skipped=%d failed=%d  "
        "projects created=%d failed=%d",
        results.total_repos,
        results.total_repos_skipped,
        results.total_workflows_created,
        results.total_workflows_skipped,
        results.total_workflows_failed,
        results.total_projects_created,
        results.total_projects_failed,
    )


if __name__ == "__main__":
    sys.exit(main())
