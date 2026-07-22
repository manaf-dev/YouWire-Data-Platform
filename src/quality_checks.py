from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHECKS = [
    "README.md",
    "lh_youwire.Lakehouse",
    "nb_bronze_to_silver.py.Notebook",
    "nb_silver_to_gold.Notebook",
    "pl_youwire_batch_daily.DataPipeline",
    "rpt_youwire_executive.Report",
    "sm_youwire_analytics.SemanticModel",
]


def _resolve_root(root: Path | str | None = None) -> Path:
    if root is None:
        return ROOT
    if isinstance(root, Path):
        return root.resolve()
    return Path(root).resolve()


def run_quality_checks(root: Path | str | None = None) -> List[Dict[str, Any]]:
    """Run the repository quality checks and return a structured result list."""
    base_dir = _resolve_root(root)
    results: List[Dict[str, Any]] = []

    for item in DEFAULT_CHECKS:
        path = base_dir / item
        exists = path.exists()
        results.append(
            {
                "name": item,
                "path": str(path),
                "exists": exists,
                "status": "PASS" if exists else "FAIL",
            }
        )

    return results


def summarize_quality_checks(
    root: Path | str | None = None,
    results: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Return a summary dictionary for the quality checks."""
    if results is None:
        results = run_quality_checks(root)

    passed = sum(1 for item in results if item.get("status") == "PASS")
    failed = len(results) - passed

    return {
        "passed": passed,
        "failed": failed,
        "total": len(results),
        "overall_status": "PASS" if failed == 0 else "FAIL",
        "results": results,
    }


def print_results(
    root: Path | str | None = None,
    results: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Print the quality check results and return the summary."""
    summary = summarize_quality_checks(root=root, results=results)
    print("Quality Check Results")
    print("-" * 24)
    for item in summary["results"]:
        print(f"[{item['status']}] {item['name']}")

    print(
        f"Overall: {summary['overall_status']} ({summary['passed']}/{summary['total']} passed)"
    )
    return summary


if __name__ == "__main__":
    print_results()
