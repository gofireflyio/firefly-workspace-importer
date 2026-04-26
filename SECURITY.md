# Security Policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, email **security@firefly.ai** with:

- A description of the issue and its impact
- Steps to reproduce (or a proof of concept)
- The commit hash or release tag where you observed the issue
- Your name and contact info if you'd like attribution

We will acknowledge receipt within 3 business days and aim to provide a
remediation plan within 10 business days.

## Scope

In scope:

- The `firefly-workspace-importer.py` script and its handling of credentials,
  configuration, and HTTP traffic
- The `Dockerfile` and the resulting container image
- Documented usage patterns in [README.md](README.md)

Out of scope:

- Vulnerabilities in third-party dependencies (report those upstream)
- Issues caused by user-supplied configuration that explicitly disables
  security controls (e.g. running the container without the documented
  hardening flags)

## Handling of credentials

The importer reads three secrets:

- `FIREFLY_ACCESS_KEY` / `FIREFLY_SECRET_KEY`
- `GITHUB_TOKEN`

These are loaded from a `.env` file or environment variables and **never**
written to logs in unredacted form (logs show `abcd...XX` masking). They are
sent only to `api.firefly.ai` (or your configured `FIREFLY_API_URL`) and
`api.github.com` over TLS.

`.env` is git-ignored and dockerignored. Do not commit it.

## Supply chain

- Runtime dependencies are pinned in [requirements.txt](requirements.txt).
- The container image is built on a distroless base
  (`gcr.io/distroless/python3-debian12:nonroot`), which has no shell or
  package manager.
- Production images should pin both base images by digest — the recipe is
  documented at the top of the [Dockerfile](Dockerfile).
- Dependabot keeps `requests`, the Docker base images, and GitHub Actions
  versions current — see [.github/dependabot.yml](.github/dependabot.yml).
