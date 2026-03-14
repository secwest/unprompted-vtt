#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TIMECODE_RE = re.compile(
    r"^(?P<start>\d{2}:\d{2}:\d{2}\.\d{3})\s+-->\s+(?P<end>\d{2}:\d{2}:\d{2}\.\d{3})$"
)


@dataclass
class Cue:
    index: int
    start: float
    end: float
    lines: list[str]

    @property
    def text(self) -> str:
        return " ".join(line.strip() for line in self.lines if line.strip()).strip()


def parse_timestamp(value: str) -> float:
    hours, minutes, seconds = value.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def format_timestamp(value: float) -> str:
    milliseconds = round(value * 1000)
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, remainder = divmod(remainder, 1_000)
    return f"{hours:02}:{minutes:02}:{seconds:02}.{remainder:03}"


def slugify(value: str, max_length: int = 96) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    collapsed = re.sub(r"-{2,}", "-", collapsed).strip("-._")
    collapsed = collapsed or "item"
    return collapsed[:max_length].rstrip("-._") or "item"


def parse_vtt(path: Path) -> list[Cue]:
    lines = path.read_text(encoding="utf-8").splitlines()
    cues: list[Cue] = []
    cursor = 0

    while cursor < len(lines):
        line = lines[cursor].strip()
        if not line or line == "WEBVTT" or line.startswith("Kind:") or line.startswith("Language:"):
            cursor += 1
            continue

        if line.isdigit():
            cursor += 1
            if cursor >= len(lines):
                break
            line = lines[cursor].strip()

        match = TIMECODE_RE.match(line)
        if not match:
            cursor += 1
            continue

        start = parse_timestamp(match.group("start"))
        end = parse_timestamp(match.group("end"))
        cursor += 1
        cue_lines: list[str] = []

        while cursor < len(lines) and lines[cursor].strip():
            cue_lines.append(lines[cursor])
            cursor += 1

        cues.append(Cue(index=len(cues) + 1, start=start, end=end, lines=cue_lines))
        cursor += 1

    return cues


def filter_cues(
    cues: list[Cue],
    *,
    start: float | None = None,
    end: float | None = None,
    pattern: re.Pattern[str] | None = None,
) -> list[Cue]:
    filtered: list[Cue] = []
    for cue in cues:
        if start is not None and cue.end < start:
            continue
        if end is not None and cue.start > end:
            continue
        if pattern is not None and not pattern.search(cue.text):
            continue
        filtered.append(cue)
    return filtered


def print_cues(cues: list[Cue], *, limit: int | None = None) -> None:
    selected = cues[:limit] if limit is not None else cues
    for cue in selected:
        print(f"{cue.index:04d} {format_timestamp(cue.start)} --> {format_timestamp(cue.end)} | {cue.text}")


def load_plan(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_vtt(path: Path, cues: list[Cue]) -> None:
    lines = ["WEBVTT", ""]
    for number, cue in enumerate(cues, start=1):
        lines.append(str(number))
        lines.append(f"{format_timestamp(cue.start)} --> {format_timestamp(cue.end)}")
        lines.extend(cue.lines)
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_txt(path: Path, cues: list[Cue]) -> None:
    lines = [f"[{format_timestamp(cue.start)} - {format_timestamp(cue.end)}] {cue.text}" for cue in cues if cue.text]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def segment_filename(segment: dict[str, Any], suffix: str) -> str:
    day = slugify(str(segment.get("day", "")) or "undated", max_length=16)
    session_block = slugify(str(segment.get("session_block", "")) or "session", max_length=32)
    sequence = str(segment.get("sequence", "")).zfill(2) or "00"
    presenter = slugify(str(segment["presenter"]), max_length=48)
    title = slugify(str(segment["title"]), max_length=48)
    role = slugify(str(segment.get("role", "presenter")), max_length=32)
    return f"{day}__{session_block}__{sequence}__{presenter}__{title}__{role}.{suffix}"


def apply_plan(plan_path: Path, output_root: Path) -> None:
    plan = load_plan(plan_path)
    repo_root = plan_path.resolve().parents[3] if "data" in str(plan_path) else Path.cwd()
    manifests_root = output_root / "metadata"
    vtt_root = output_root / "vtt"
    txt_root = output_root / "txt"
    manifest_rows: list[dict[str, Any]] = []

    cue_cache: dict[Path, list[Cue]] = {}
    for segment in plan.get("segments", []):
        source_file = (repo_root / segment["source_file"]).resolve()
        if source_file not in cue_cache:
            cue_cache[source_file] = parse_vtt(source_file)

        cues = cue_cache[source_file]
        start = parse_timestamp(segment["start"])
        end = parse_timestamp(segment["end"])
        segment_cues = [cue for cue in cues if cue.end > start and cue.start < end]
        if not segment_cues:
            raise SystemExit(f"No cues matched segment {segment['id']} from {source_file}.")

        vtt_name = segment_filename(segment, "vtt")
        txt_name = segment_filename(segment, "txt")
        vtt_path = vtt_root / vtt_name
        txt_path = txt_root / txt_name
        write_vtt(vtt_path, segment_cues)
        write_txt(txt_path, segment_cues)

        manifest_rows.append(
            {
                "id": segment["id"],
                "day": segment.get("day", ""),
                "session_block": segment.get("session_block", ""),
                "sequence": segment.get("sequence", ""),
                "presenter": segment["presenter"],
                "title": segment["title"],
                "role": segment.get("role", "presenter"),
                "source_file": str(source_file),
                "start": segment["start"],
                "end": segment["end"],
                "output_vtt": str(vtt_path),
                "output_txt": str(txt_path),
                "notes": segment.get("notes", ""),
                "divergence": segment.get("divergence", ""),
            }
        )

    manifests_root.mkdir(parents=True, exist_ok=True)
    manifest_path = manifests_root / "presenter_segments.csv"
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "id",
                "day",
                "session_block",
                "sequence",
                "presenter",
                "title",
                "role",
                "source_file",
                "start",
                "end",
                "output_vtt",
                "output_txt",
                "notes",
                "divergence",
            ],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)

    summary = {
        "plan": str(plan_path),
        "segments_written": len(manifest_rows),
        "presenters_root": str(output_root),
        "manifest_csv": str(manifest_path),
    }
    (manifests_root / "presenter_segments_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect and split VTT transcripts by a reviewed presenter plan.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect", help="Inspect cues from a VTT transcript.")
    inspect_parser.add_argument("file", help="Path to a .vtt file.")
    inspect_parser.add_argument("--from", dest="from_time", help="Only show cues at/after this timestamp.")
    inspect_parser.add_argument("--to", dest="to_time", help="Only show cues at/before this timestamp.")
    inspect_parser.add_argument("--search", help="Only show cues matching this regex.")
    inspect_parser.add_argument("--limit", type=int, help="Limit the number of cues displayed.")

    split_parser = subparsers.add_parser("split", help="Apply a presenter split plan.")
    split_parser.add_argument("--plan", required=True, help="Path to presenter_split_plan.json.")
    split_parser.add_argument("--output-root", required=True, help="Directory where split files should be written.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "inspect":
        file_path = Path(args.file).resolve()
        cues = parse_vtt(file_path)
        start = parse_timestamp(args.from_time) if args.from_time else None
        end = parse_timestamp(args.to_time) if args.to_time else None
        pattern = re.compile(args.search, re.IGNORECASE) if args.search else None
        print_cues(filter_cues(cues, start=start, end=end, pattern=pattern), limit=args.limit)
        return 0

    if args.command == "split":
        apply_plan(Path(args.plan).resolve(), Path(args.output_root).resolve())
        return 0

    parser.error("Unknown command.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
