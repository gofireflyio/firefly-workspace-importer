#!/usr/bin/env python3
"""
Firefly Workspace Importer
==========================

End-to-end tool to import GitHub repositories containing Terraform code
into Firefly as workspaces and projects.

Two-step pipeline (run separately or together):
  1. Scan GitHub repositories for directories containing `.tf` files and
     emit a JSON mapping.
  2. Create one Firefly workspace per leaf directory, optionally mirroring
     the directory tree as a Firefly project hierarchy.

Subcommands:
  map     - Scan GitHub and write the directory mapping JSON.
  create  - Read an existing mapping JSON and create Firefly resources.
  run     - map + create in one go.

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
from urllib.parse import urlparse

import requests

# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------

DEFAULT_FIREFLY_API_URL = "https://api.firefly.ai"
DEFAULT_MAPPING_FILE = "github_directory_mapping.json"
DEFAULT_RESULTS_FILE = "firefly_workflows_created.json"
DEFAULT_ENV_FILE = ".env"
DEFAULT_CONFIG_FILE = "config.json"

VALID_VCS_TYPES = {"github", "gitlab", "bitbucket", "codecommit", "azuredevops"}
VALID_RUNNER_TYPES = {
    "github-actions", "gitlab-pipelines", "bitbucket-pipelines",
    "azure-pipelines", "jenkins", "semaphore", "atlantis",
    "env0", "firefly", "unrecognized",
}
VALID_TRIGGERS = {"merge", "push", "pull_request"}
VALID_APPLY_RULES = {"manual", "auto"}
VALID_SENSITIVITIES = {"string", "secret"}
VALID_DESTINATIONS = {"env", "iac"}

HTTP_RETRY_STATUSES = {429, 500, 502, 503, 504}
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_BASE = 1.5  # seconds; exponential

EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_CONFIG = 2
EXIT_AUTH = 3

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


# ----------------------------------------------------------------------------
# Config dataclasses
# ----------------------------------------------------------------------------

@dataclass
class Secrets:
    firefly_access_key: str
    firefly_secret_key: str
    github_token: Optional[str]
    firefly_api_url: str = DEFAULT_FIREFLY_API_URL


@dataclass
class VcsConfig:
    id: str
    type: str
    default_branch: str = "main"


@dataclass
class WorkspaceConfig:
    runner_type: str = "firefly"
    iac_type: str = "terraform"
    terraform_version: str = "1.5.7"
    execution_triggers: list[str] = field(default_factory=lambda: ["merge"])
    apply_rule: str = "manual"
    variables: list[dict] = field(default_factory=list)
    consumed_variable_sets: list[str] = field(default_factory=list)


@dataclass
class ProjectsConfig:
    create: bool = True
    project_id: Optional[str] = None
    main_members: list[dict] = field(default_factory=list)
    main_variables: list[dict] = field(default_factory=list)
    path_members: dict[str, list[dict]] = field(default_factory=dict)
    path_variables: dict[str, list[dict]] = field(default_factory=dict)


@dataclass
class Config:
    repos: list[str]
    vcs: VcsConfig
    workspace: WorkspaceConfig
    projects: ProjectsConfig


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
    github = get("GITHUB_TOKEN") or get("GH_TOKEN")

    if not access or not secret:
        raise ConfigError(
            f"Missing FIREFLY_ACCESS_KEY or FIREFLY_SECRET_KEY. "
            f"Set them in {env_path} or in the environment."
        )

    return Secrets(
        firefly_access_key=access,
        firefly_secret_key=secret,
        github_token=github,
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

    repos = raw.get("repos") or []
    vcs = raw.get("vcs") or {}
    ws = raw.get("workspace") or {}
    pr = raw.get("projects") or {}

    cfg = Config(
        repos=list(repos),
        vcs=VcsConfig(
            id=vcs.get("id", ""),
            type=vcs.get("type", ""),
            default_branch=vcs.get("default_branch", "main"),
        ),
        workspace=WorkspaceConfig(
            runner_type=ws.get("runner_type", "firefly"),
            iac_type=ws.get("iac_type", "terraform"),
            terraform_version=ws.get("terraform_version", "1.5.7"),
            execution_triggers=list(ws.get("execution_triggers", ["merge"])),
            apply_rule=ws.get("apply_rule", "manual"),
            variables=list(ws.get("variables", [])),
            consumed_variable_sets=list(ws.get("consumed_variable_sets", [])),
        ),
        projects=ProjectsConfig(
            create=bool(pr.get("create", True)),
            project_id=pr.get("project_id"),
            main_members=list(pr.get("main_members", [])),
            main_variables=list(pr.get("main_variables", [])),
            path_members=dict(pr.get("path_members", {})),
            path_variables=dict(pr.get("path_variables", {})),
        ),
    )
    return cfg


def validate_config(cfg: Config, *, require_repos: bool, require_firefly: bool) -> None:
    errors: list[str] = []

    if require_repos and not cfg.repos:
        errors.append("config.repos is empty")

    if require_firefly:
        if not cfg.vcs.id:
            errors.append("config.vcs.id is required")
        if cfg.vcs.type not in VALID_VCS_TYPES:
            errors.append(
                f"config.vcs.type must be one of {sorted(VALID_VCS_TYPES)}, "
                f"got {cfg.vcs.type!r}"
            )
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
        for label, members_list in (("projects.main_members", cfg.projects.main_members),):
            errors.extend(_validate_members(label, members_list))
        for path, members_list in cfg.projects.path_members.items():
            errors.extend(_validate_members(f"projects.path_members[{path!r}]", members_list))

    if errors:
        raise ConfigError("Configuration errors:\n  - " + "\n  - ".join(errors))


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
            errs.append(
                f"{label}[{i}].sensitivity must be one of {sorted(VALID_SENSITIVITIES)}"
            )
        dest = v.get("destination", "env")
        if dest not in VALID_DESTINATIONS:
            errs.append(
                f"{label}[{i}].destination must be one of {sorted(VALID_DESTINATIONS)}"
            )
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
            log.debug(
                "HTTP %s on %s (attempt %d); retry in %.1fs",
                resp.status_code, url, attempt + 1, delay,
            )
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
# GitHub client
# ----------------------------------------------------------------------------

def parse_repo_input(repo_input: str) -> tuple[str, Optional[str], bool]:
    """Parse "owner", "owner/repo", or full GitHub URL. Returns (owner, repo, is_org)."""
    s = repo_input.strip()
    if s.startswith("http"):
        parts = urlparse(s).path.strip("/").split("/")
        if len(parts) >= 2:
            return parts[0], parts[1].removesuffix(".git"), False
        if len(parts) == 1 and parts[0]:
            return parts[0], None, True
        raise ConfigError(f"Cannot parse repo URL: {repo_input!r}")
    if "/" in s:
        owner, _, repo = s.partition("/")
        if owner and repo:
            return owner, repo, False
    if s:
        return s, None, True
    raise ConfigError(f"Empty repo entry")


class GitHubClient:
    """Minimal GitHub REST client for directory mapping."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: Optional[str]):
        self.session = requests.Session()
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "firefly-workspace-automation",
        }
        if token:
            headers["Authorization"] = f"token {token}"
        self.session.headers.update(headers)
        self.authenticated = bool(token)

    def list_org_repos(self, org: str) -> list[str]:
        repos: list[str] = []
        page = 1
        while True:
            url = f"{self.BASE_URL}/orgs/{org}/repos"
            resp = self._get(url, params={"page": page, "per_page": 100, "type": "all"})
            batch = resp.json()
            if not batch:
                break
            for item in batch:
                repos.append(f"{item['owner']['login']}/{item['name']}")
            if len(batch) < 100:
                break
            page += 1
        return repos

    def map_repo_directories(
        self, owner: str, repo: str, branch: Optional[str] = None
    ) -> dict[str, Any]:
        """Return a nested dict of directories that contain `.tf` files (and parents)."""
        if not branch:
            info = self._get(f"{self.BASE_URL}/repos/{owner}/{repo}").json()
            branch = info.get("default_branch", "main")

        ref_url = f"{self.BASE_URL}/repos/{owner}/{repo}/git/ref/heads/{branch}"
        ref_resp = self._get(ref_url, allow_404=True)
        if ref_resp.status_code == 404:
            raise ApiError(
                f"branch {branch!r} not found for {owner}/{repo} "
                f"(repo missing or no access)"
            )
        tree_sha = ref_resp.json()["object"]["sha"]

        tree_url = f"{self.BASE_URL}/repos/{owner}/{repo}/git/trees/{tree_sha}"
        tree = self._get(tree_url, params={"recursive": "1"}).json()
        if tree.get("truncated"):
            log.warning("GitHub tree for %s/%s is truncated; some dirs may be missing",
                        owner, repo)

        tf_paths = [
            item["path"] for item in tree.get("tree", [])
            if item.get("type") == "blob" and item.get("path", "").endswith(".tf")
        ]

        included: set[str] = set()
        for path in tf_paths:
            if path.startswith("."):
                continue
            if "/" not in path:
                continue
            dir_path = path.rsplit("/", 1)[0]
            parts = dir_path.split("/")
            for i in range(len(parts)):
                included.add("/".join(parts[: i + 1]))

        mapping: dict[str, Any] = {}
        for dir_path in sorted(included):
            if dir_path.startswith("."):
                continue
            cursor = mapping
            for part in dir_path.split("/"):
                cursor = cursor.setdefault(part, {})
        return mapping

    def _get(self, url: str, *, params: Optional[dict] = None, allow_404: bool = False) -> requests.Response:
        self._respect_rate_limit()
        resp = request_with_retry(self.session, "GET", url, params=params, timeout=30)
        if allow_404 and resp.status_code == 404:
            return resp
        if not resp.ok:
            raise ApiError(f"GitHub {resp.status_code} on {url}: {resp.text[:200]}")
        return resp

    def _respect_rate_limit(self) -> None:
        # If the previous response left us at 0 remaining, sleep until reset.
        # We piggy-back on session-stored last-response by checking after each call.
        pass  # Implemented by inspecting headers after each call below if needed.


# ----------------------------------------------------------------------------
# Firefly client
# ----------------------------------------------------------------------------

class FireflyClient:
    """Firefly v2 API client."""

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

    # --- Projects ---

    def get_projects_tree(self, search_query: Optional[str] = None) -> dict[str, Any]:
        url = f"{self.base_url}/v2/runners/projects/tree"
        params = {"searchQuery": search_query} if search_query else None
        resp = request_with_retry(self.session, "GET", url, params=params, timeout=30)
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
            return {
                "success": False,
                "status_code": resp.status_code,
                "error": resp.text[:500],
            }
        try:
            data = resp.json() if resp.content else {}
        except ValueError:
            data = {}
        return {"success": True, "status_code": resp.status_code, "data": data}


# ----------------------------------------------------------------------------
# Path / mapping helpers
# ----------------------------------------------------------------------------

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


def format_work_dir(work_dir: str) -> str:
    return work_dir if work_dir.startswith("/") else f"/{work_dir}"


def sanitize_project_name(name: str) -> str:
    """Project names: alphanumeric + - and _; no spaces or slashes."""
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
    """Exact match first, else longest matching parent path."""
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
# Orchestrator
# ----------------------------------------------------------------------------

@dataclass
class Results:
    total_repos: int = 0
    total_workflows_created: int = 0
    total_workflows_failed: int = 0
    total_projects_created: int = 0
    workflows: list[dict] = field(default_factory=list)
    projects: list[dict] = field(default_factory=list)


class Orchestrator:
    def __init__(
        self,
        github: GitHubClient,
        firefly: FireflyClient,
        config: Config,
        *,
        dry_run: bool,
        results_path: Path,
        workers: int,
    ):
        self.github = github
        self.firefly = firefly
        self.config = config
        self.dry_run = dry_run
        self.results_path = results_path
        self.workers = max(1, workers)

    # --- map ---

    def build_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {}
        for entry in self.config.repos:
            owner, repo, is_org = parse_repo_input(entry)
            if is_org:
                log.info("Listing repos for organization %s", owner)
                try:
                    repo_list = self.github.list_org_repos(owner)
                except ApiError as e:
                    log.error("Skipping org %s: %s", owner, e)
                    mapping[f"{owner}/*"] = {"error": str(e)}
                    continue
                log.info("Found %d repos in %s", len(repo_list), owner)
                for full in repo_list:
                    o, r = full.split("/", 1)
                    mapping[full] = self._safe_map(o, r)
            else:
                assert repo is not None
                mapping[f"{owner}/{repo}"] = self._safe_map(owner, repo)
        return mapping

    def _safe_map(self, owner: str, repo: str) -> dict[str, Any]:
        log.info("Mapping %s/%s", owner, repo)
        try:
            result = self.github.map_repo_directories(owner, repo)
            log.info("  %s/%s: %d directories", owner, repo, _count_dirs(result))
            return result
        except ApiError as e:
            log.error("  %s/%s failed: %s", owner, repo, e)
            return {"error": str(e)}

    # --- create ---

    def create_all(self, mapping: dict[str, Any]) -> Results:
        results = Results(total_repos=len(mapping))

        for repo, structure in mapping.items():
            log.info("Repository: %s", repo)
            if not isinstance(structure, dict) or "error" in structure:
                log.warning("  Skipping %s (mapping error: %s)",
                            repo, structure.get("error") if isinstance(structure, dict) else "invalid")
                continue

            project_map: dict[str, str] = {}
            root_project_id: Optional[str] = None

            if self.config.projects.create:
                root_project_id, project_map, created_count = self._build_projects_for_repo(repo, structure)
                results.total_projects_created += created_count

            self._apply_path_attachments(project_map)

            leaf_dirs = get_leaf_directories(structure)
            log.info("  %d leaf directories", len(leaf_dirs))
            self._create_workspaces_for_repo(repo, leaf_dirs, project_map, results)

            if self.config.projects.create:
                results.projects.append({
                    "repo": repo,
                    "root_project_id": root_project_id,
                    "project_map": project_map,
                })

            self._save_results(results)

        return results

    def _build_projects_for_repo(
        self, repo: str, structure: dict[str, Any]
    ) -> tuple[Optional[str], dict[str, str], int]:
        created = 0

        if self.dry_run:
            log.info("  [dry-run] would build project tree for %s", repo)
            project_map = {format_work_dir(p): f"<dry-run:{sanitize_project_name(workspace_name_for(repo, p))}>"
                           for p in _all_paths(structure)}
            return f"<dry-run:{sanitize_project_name(repo)}>", project_map, 0

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
                return existing_root_id, {}, 0
            root_project_id = res["data"].get("id")
            created += 1
            self._apply_main_attachments(root_project_id)

        project_map: dict[str, str] = {}
        created += self._build_subtree(structure, repo, "", root_project_id, existing_tree, project_map)
        return root_project_id, project_map, created

    def _build_subtree(
        self,
        structure: dict[str, Any],
        repo: str,
        base_path: str,
        parent_id: Optional[str],
        existing_tree: dict[str, Any],
        project_map: dict[str, str],
    ) -> int:
        created = 0
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
                    continue
                project_id = res["data"].get("id")
                project_map[formatted] = project_id
                created += 1

            if subdirs and project_id:
                created += self._build_subtree(
                    subdirs, repo, current, project_id, existing_tree, project_map
                )
        return created

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
        leaf_dirs: list[str],
        project_map: dict[str, str],
        results: Results,
    ) -> None:
        bodies: list[tuple[str, str, dict]] = []
        for work_dir in leaf_dirs:
            formatted = format_work_dir(work_dir)
            ws_name = workspace_name_for(repo, work_dir)
            project_id = (
                project_id_for_work_dir(formatted, project_map)
                if self.config.projects.create else None
            ) or self.config.projects.project_id

            body: dict[str, Any] = {
                "runnerType": self.config.workspace.runner_type,
                "iacType": self.config.workspace.iac_type,
                "workspaceName": ws_name,
                "vcsId": self.config.vcs.id,
                "repo": repo,
                "defaultBranch": self.config.vcs.default_branch,
                "vcsType": self.config.vcs.type,
                "workDir": formatted,
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
                self._submit_one(repo, ws_name, formatted, body, results)
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

    def _submit_one(
        self, repo: str, ws_name: str, formatted: str, body: dict, results: Results,
    ) -> None:
        log.info("  Creating workspace: %s", ws_name)
        res = self.firefly.create_workspace(body)
        self._record_workspace(repo, ws_name, formatted, body, res, results)

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
            "total_workflows_created": results.total_workflows_created,
            "total_workflows_failed": results.total_workflows_failed,
            "total_projects_created": results.total_projects_created,
            "workflows": results.workflows,
            "projects": results.projects,
        }
        tmp = self.results_path.with_suffix(self.results_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.results_path)


def _count_dirs(structure: dict[str, Any]) -> int:
    count = 0
    for value in structure.values():
        if isinstance(value, dict):
            count += 1
            count += _count_dirs(value)
    return count


def _all_paths(structure: dict[str, Any], base: str = "") -> list[str]:
    out: list[str] = []
    for name, sub in structure.items():
        path = f"{base}/{name}" if base else name
        out.append(path)
        if isinstance(sub, dict) and sub:
            out.extend(_all_paths(sub, path))
    return out


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="firefly-workspace-importer.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--env-file", default=DEFAULT_ENV_FILE,
                   help=f"Path to .env file with secrets (default: {DEFAULT_ENV_FILE})")
    p.add_argument("--config", default=DEFAULT_CONFIG_FILE,
                   help=f"Path to JSON config file (default: {DEFAULT_CONFIG_FILE})")
    p.add_argument("--mapping-file", default=DEFAULT_MAPPING_FILE,
                   help=f"Path to mapping JSON (default: {DEFAULT_MAPPING_FILE})")
    p.add_argument("--results-file", default=DEFAULT_RESULTS_FILE,
                   help=f"Path to results JSON (default: {DEFAULT_RESULTS_FILE})")
    p.add_argument("--workers", type=int, default=1,
                   help="Concurrent workspace creations (default: 1, serial)")
    p.add_argument("--dry-run", action="store_true",
                   help="Do not call write APIs; show what would happen")
    p.add_argument("-v", "--verbose", action="count", default=1,
                   help="Increase verbosity (-v info [default], -vv debug)")
    p.add_argument("-q", "--quiet", action="store_true",
                   help="Only warnings and errors")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("map", help="Scan GitHub repos and write mapping JSON")
    sub.add_parser("create", help="Read mapping JSON and create Firefly resources")
    sub.add_parser("run", help="map + create end-to-end")
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    setup_logging(0 if args.quiet else args.verbose)

    env_path = Path(args.env_file)
    config_path = Path(args.config)
    mapping_path = Path(args.mapping_file)
    results_path = Path(args.results_file)

    try:
        secrets = load_secrets(env_path)
        config = load_config(config_path)
    except ConfigError as e:
        log.error("%s", e)
        return EXIT_CONFIG

    log.info("Firefly API: %s", secrets.firefly_api_url)
    log.info("Firefly access key: %s", mask(secrets.firefly_access_key))
    log.info("GitHub token: %s", mask(secrets.github_token) if secrets.github_token else "<unset, public-only>")
    if args.dry_run:
        log.warning("DRY-RUN: no write APIs will be called")

    try:
        if args.command == "map":
            validate_config(config, require_repos=True, require_firefly=False)
            github = GitHubClient(secrets.github_token)
            mapping = Orchestrator(github, _NoopFirefly(), config,
                                   dry_run=args.dry_run, results_path=results_path,
                                   workers=args.workers).build_mapping()
            mapping_path.write_text(json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8")
            log.warning("Wrote mapping to %s", mapping_path)
            return EXIT_OK

        if args.command == "create":
            validate_config(config, require_repos=False, require_firefly=True)
            if not mapping_path.exists():
                log.error("Mapping file not found: %s. Run `map` first.", mapping_path)
                return EXIT_CONFIG
            mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
            firefly = FireflyClient(secrets)
            if not args.dry_run:
                firefly.login()
            orch = Orchestrator(GitHubClient(secrets.github_token), firefly, config,
                                dry_run=args.dry_run, results_path=results_path,
                                workers=args.workers)
            results = orch.create_all(mapping)
            _print_summary(results)
            return EXIT_OK if results.total_workflows_failed == 0 else EXIT_PARTIAL

        if args.command == "run":
            validate_config(config, require_repos=True, require_firefly=True)
            github = GitHubClient(secrets.github_token)
            firefly = FireflyClient(secrets)
            if not args.dry_run:
                firefly.login()
            orch = Orchestrator(github, firefly, config,
                                dry_run=args.dry_run, results_path=results_path,
                                workers=args.workers)
            mapping = orch.build_mapping()
            mapping_path.write_text(json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8")
            log.warning("Wrote mapping to %s", mapping_path)
            results = orch.create_all(mapping)
            _print_summary(results)
            return EXIT_OK if results.total_workflows_failed == 0 else EXIT_PARTIAL

    except ConfigError as e:
        log.error("%s", e)
        return EXIT_CONFIG
    except AuthError as e:
        log.error("%s", e)
        return EXIT_AUTH
    except KeyboardInterrupt:
        log.error("Interrupted")
        return EXIT_PARTIAL

    return EXIT_OK


def _print_summary(results: Results) -> None:
    log.warning("Summary: repos=%d  workspaces created=%d  failed=%d  projects created=%d",
                results.total_repos,
                results.total_workflows_created,
                results.total_workflows_failed,
                results.total_projects_created)


class _NoopFirefly:
    """Placeholder used by `map` so the orchestrator type-checks without a real client."""

    def __getattr__(self, name: str) -> Any:
        raise RuntimeError(f"Firefly client not initialized (called {name})")


if __name__ == "__main__":
    sys.exit(main())
