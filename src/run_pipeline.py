import subprocess
import sys


SCRIPTS = [
    "src/phase1_movielens.py",
    "src/phase2_imdb_enrichment.py",
    "src/phase3_filter_and_rebuild.py",
    "src/phase4_tmdb_enrichment.py",
    "src/phase5_finalize_exports.py",
]


def run_script(script_path: str) -> None:
    print(f"\nRunning {script_path} ...")
    result = subprocess.run([sys.executable, script_path], check=False)

    if result.returncode != 0:
        raise RuntimeError(f"Pipeline stopped: {script_path} failed with exit code {result.returncode}")


def main() -> None:
    for script in SCRIPTS:
        run_script(script)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()