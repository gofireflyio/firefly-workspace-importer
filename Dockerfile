# syntax=docker/dockerfile:1.7

# =============================================================================
# Firefly Workspace Importer - hardened container image
# =============================================================================
#
# Multi-stage build:
#   1. build   - python:3.11-slim-bookworm; installs deps to a flat target dir.
#   2. runtime - gcr.io/distroless/python3-debian12:nonroot
#                (no shell, no apt, no package manager; runs as UID 65532).
#
# Build:
#     docker build -t firefly-workspace-importer:latest .
#
# Run (mount cwd so the container can read .env / config.json and write output):
#     docker run --rm \
#       --read-only \
#       --cap-drop=ALL \
#       --security-opt=no-new-privileges \
#       --network=bridge \
#       -v "$(pwd):/work" \
#       --env-file .env \
#       firefly-workspace-importer:latest run
#
# Runtime hardening flags above:
#     --read-only                       root FS is read-only; only /work (mount) is writable
#     --cap-drop=ALL                    drop every Linux capability (none are needed)
#     --security-opt=no-new-privileges  block setuid/setgid escalation
#     --network=bridge                  default; consider --network=none if you only need `map`
#
# To pin base images by digest (strongly recommended for production):
#     docker pull python:3.11-slim-bookworm
#     docker inspect --format='{{index .RepoDigests 0}}' python:3.11-slim-bookworm
#     docker pull gcr.io/distroless/python3-debian12:nonroot
#     docker inspect --format='{{index .RepoDigests 0}}' gcr.io/distroless/python3-debian12:nonroot
# Then replace the FROM lines below with the @sha256:... form and commit.
# =============================================================================


# ---- Stage 1: build ---------------------------------------------------------
FROM python:3.11-slim-bookworm AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /build

# Install dependencies into a flat directory we can copy into the runtime image.
# Doing this before copying the script keeps the layer cached when only the
# script changes.
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/install -r requirements.txt


# ---- Stage 2: runtime (distroless) ------------------------------------------
# Distroless ships Python 3.11; the build stage version must match.
FROM gcr.io/distroless/python3-debian12:nonroot

# OCI labels for provenance / scanners / registries.
LABEL org.opencontainers.image.title="Firefly Workspace Importer" \
      org.opencontainers.image.description="Import GitHub Terraform repositories into Firefly as workspaces and projects" \
      org.opencontainers.image.source="https://github.com/gofireflyio/firefly-workspace-importer" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.vendor="Firefly" \
      org.opencontainers.image.base.name="gcr.io/distroless/python3-debian12:nonroot"

# Copy installed site-packages from the build stage and the application script.
# Both owned by `nonroot` (UID 65532) to play well with --read-only root FS.
COPY --from=build --chown=nonroot:nonroot /install /app/deps
COPY --chown=nonroot:nonroot firefly-workspace-importer.py /app/firefly-workspace-importer.py

ENV PYTHONPATH=/app/deps \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random

# Drop privileges. Distroless `:nonroot` already sets USER 65532 by default, but
# being explicit makes the security posture obvious to scanners and reviewers.
USER nonroot:nonroot

# /work is the user's mount point: the script reads ./config.json and ./.env
# from here and writes its mapping/results files here.
WORKDIR /work

ENTRYPOINT ["python3", "/app/firefly-workspace-importer.py"]
CMD ["--help"]
