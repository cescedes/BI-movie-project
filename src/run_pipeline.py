import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SCRIPTS = [
    PROJECT_ROOT / "src" / "phase1_movielens.py",
    PROJECT_ROOT / "src" / "phase2_imdb_enrichment.py",
    PROJECT_ROOT / "src" / "phase3_filter_and_rebuild.py",
    PROJECT_ROOT / "src" / "phase4_tmdb_enrichment.py",
    PROJECT_ROOT / "src" / "phase5_finalize_exports.py",
]


def run_script(script_path: Path) -> None:
    print(f"\nRunning {script_path.name} ...")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        check=False,
        cwd=PROJECT_ROOT,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline stopped: {script_path.name} failed with exit code {result.returncode}"
        )


def main() -> None:
    for script in SCRIPTS:
        run_script(script)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()