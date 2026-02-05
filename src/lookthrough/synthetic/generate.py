from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

import yaml


@dataclass(frozen=True)
class Paths:
    repo_root: Path
    data_silver: Path


def load_config(config_path: Path) -> Dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dirs(paths: Paths) -> None:
    paths.data_silver.mkdir(parents=True, exist_ok=True)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    config_path = repo_root / "src" / "lookthrough" / "synthetic" / "config.yaml"
    cfg = load_config(config_path)

    paths = Paths(
        repo_root=repo_root,
        data_silver=repo_root / "data" / "silver",
    )
    ensure_dirs(paths)

    print("Synthetic generator scaffold ready.")
    print(f"Config loaded: counts={cfg.get('v1', {}).get('counts')}")
    print(f"Writing outputs to: {paths.data_silver}")


if __name__ == "__main__":
    main()
