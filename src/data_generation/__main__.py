"""Allow ``python -m src.data_generation`` to work as a CLI entry-point."""

import logging
from pathlib import Path

from .generator import SandboxOrchestrator


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
    )

    # Walk upward from this file to find config/settings.yaml
    project_root = Path(__file__).resolve().parent.parent.parent
    config_path = project_root / "config" / "settings.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Settings file not found at {config_path}. "
            "Please ensure config/settings.yaml exists."
        )

    orchestrator = SandboxOrchestrator(config_path)
    orchestrator.run()


if __name__ == "__main__":
    main()