#!/usr/bin/env python3

import importlib.metadata
import re
import sys
import tomllib
from pathlib import Path


def bootstrap_local_dependency(repo_name: str) -> None:
    repo_root = Path(__file__).resolve().parent
    candidate = repo_root.parent / repo_name
    if not candidate.is_dir():
        return

    nested_src = candidate / repo_name
    if (nested_src / repo_name / "__init__.py").is_file():
        candidate_path = str(nested_src)
    else:
        candidate_path = str(candidate)

    if candidate_path not in sys.path:
        sys.path.insert(0, candidate_path)


def _version_key(version: str) -> tuple:
    parts = []
    for token in re.split(r"[.+-]", str(version or "").strip()):
        if not token:
            continue
        if token.isdigit():
            parts.append((0, int(token)))
        else:
            parts.append((1, token.lower()))
    return tuple(parts)


def _local_repo_version(repo_name: str) -> str | None:
    repo_root = Path(__file__).resolve().parent
    pyproject_path = repo_root.parent / repo_name / "pyproject.toml"
    if not pyproject_path.is_file():
        return None
    try:
        with pyproject_path.open("rb") as handle:
            payload = tomllib.load(handle)
        return str(payload.get("project", {}).get("version") or "").strip() or None
    except Exception:
        return None


def dependency_version(repo_name: str) -> str | None:
    local_version = _local_repo_version(repo_name)
    if local_version:
        return local_version
    try:
        return importlib.metadata.version(repo_name)
    except importlib.metadata.PackageNotFoundError:
        return None


def ensure_dependency_version(repo_name: str, minimum_version: str) -> str:
    version = dependency_version(repo_name)
    if version is None:
        raise ModuleNotFoundError(f"{repo_name} is not installed")
    if _version_key(version) < _version_key(minimum_version):
        raise RuntimeError(f"{repo_name} {minimum_version}+ required, found {version}")
    return version
