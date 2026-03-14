#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
README_PATH = REPO_ROOT / "README.md"
MANIFEST_PATH = REPO_ROOT / "data" / "unpromptedcon-org" / "presenters" / "metadata" / "presenter_segments.csv"
BEGIN_MARKER = "<!-- BEGIN PRESENTER TRANSCRIPT INDEX -->"
END_MARKER = "<!-- END PRESENTER TRANSCRIPT INDEX -->"


def escape_markdown(value: str) -> str:
    return value.replace("|", r"\|").replace("\n", " ").strip()


def to_repo_relative(value: str) -> str:
    path = Path(value)
    if path.is_absolute():
        path = path.relative_to(REPO_ROOT)
    return path.as_posix()


def build_index_section() -> str:
    rows = [
        "## Transcript file index",
        "",
        "Generated from `data/unpromptedcon-org/presenters/metadata/presenter_segments.csv`.",
        "",
        "| Day | Session block | Speaker | Presentation | Role | Files |",
        "| --- | --- | --- | --- | --- | --- |",
    ]

    with MANIFEST_PATH.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            txt_link = to_repo_relative(row["output_txt"])
            vtt_link = to_repo_relative(row["output_vtt"])
            rows.append(
                "| "
                + " | ".join(
                    [
                        escape_markdown(row["day"]),
                        escape_markdown(row["session_block"]),
                        escape_markdown(row["presenter"]),
                        escape_markdown(row["title"]),
                        escape_markdown(row["role"]),
                        f"[txt]({txt_link}) / [vtt]({vtt_link})",
                    ]
                )
                + " |"
            )

    return "\n".join(rows)


def update_readme() -> None:
    section = f"{BEGIN_MARKER}\n\n{build_index_section()}\n\n{END_MARKER}"
    readme = README_PATH.read_text(encoding="utf-8")

    if BEGIN_MARKER in readme and END_MARKER in readme:
        start = readme.index(BEGIN_MARKER)
        end = readme.index(END_MARKER) + len(END_MARKER)
        updated = readme[:start] + section + readme[end:]
    else:
        if "\n## Repository layout" in readme:
            insert_at = readme.index("\n## Repository layout")
            updated = readme[:insert_at].rstrip() + "\n\n" + section + "\n" + readme[insert_at:]
        else:
            updated = readme.rstrip() + "\n\n" + section + "\n"

    README_PATH.write_text(updated, encoding="utf-8")


def main() -> int:
    if not MANIFEST_PATH.exists():
        raise SystemExit(f"Manifest not found: {MANIFEST_PATH}")
    update_readme()
    print(f"Updated {README_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
