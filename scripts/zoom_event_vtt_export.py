#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

API_BASE = "https://api.zoom.us/v2"
TOKEN_URL = "https://zoom.us/oauth/token"
DEFAULT_PAGE_SIZE = 300
DEFAULT_TIMEOUT_SECONDS = 60
MAX_WINDOW_DAYS = 30
TRANSCRIPT_FILE_TYPES = {"CC", "TRANSCRIPT", "VTT"}
TRANSCRIPT_RECORDING_TYPES = {"audio_transcript", "cc", "closed_caption", "closed_captions"}


class ZoomApiError(RuntimeError):
    def __init__(self, url: str, status_code: int | None, body: bytes | None = None):
        self.url = url
        self.status_code = status_code
        self.body = body or b""
        body_preview = self.body.decode("utf-8", errors="replace").strip()
        if len(body_preview) > 200:
            body_preview = f"{body_preview[:197]}..."
        status_text = status_code if status_code is not None else "network-error"
        super().__init__(f"{url} -> {status_text}: {body_preview}")


class TokenPreservingRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, D401
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is None:
            return None

        auth_value = req.headers.get("Authorization") or req.unredirected_hdrs.get("Authorization")
        if auth_value:
            redirected.add_unredirected_header("Authorization", auth_value)

        return redirected


def print_status(message: str) -> None:
    print(message, file=sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Zoom VTT transcript files for recordings that match an event-style filter.",
    )
    parser.add_argument("--from", dest="from_date", required=True, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--to", dest="to_date", required=True, help="End date (YYYY-MM-DD).")
    parser.add_argument(
        "--event-slug",
        help="Folder name under data\\ for this export. Defaults to zoom-export-<from>-to-<to>.",
    )
    parser.add_argument("--account-id", help="Zoom account ID. Defaults to ZOOM_ACCOUNT_ID from .env.")
    parser.add_argument("--client-id", help="Zoom client ID. Defaults to ZOOM_CLIENT_ID from .env.")
    parser.add_argument("--client-secret", help="Zoom client secret. Defaults to ZOOM_CLIENT_SECRET from .env.")
    parser.add_argument(
        "--user-id",
        help="Optional Zoom user email or user ID. If set, list recordings for that user instead of the whole account.",
    )
    parser.add_argument(
        "--host-email",
        action="append",
        default=[],
        help="Match only recordings hosted by this email address. Repeat for multiple hosts.",
    )
    parser.add_argument(
        "--topic-contains",
        action="append",
        default=[],
        help="Match recordings whose topic contains this text. Repeat for multiple alternatives.",
    )
    parser.add_argument(
        "--topic-regex",
        help="Optional case-insensitive regex to apply to the meeting topic after other filters.",
    )
    parser.add_argument(
        "--meeting-id",
        action="append",
        default=[],
        help="Match an explicit meeting ID or UUID. Repeat for multiple values.",
    )
    parser.add_argument(
        "--exclude-meeting-id",
        action="append",
        default=[],
        help="Exclude an explicit meeting ID or UUID. Repeat for multiple values.",
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="Base directory for output. Defaults to data\\ in the repository root.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Zoom page size (1-{DEFAULT_PAGE_SIZE}). Defaults to {DEFAULT_PAGE_SIZE}.",
    )
    parser.add_argument(
        "--include-trash",
        action="store_true",
        help="Include trashed recordings if the endpoint supports it.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write manifests but skip the actual VTT downloads.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Redownload files even if they already exist locally.",
    )
    return parser.parse_args()


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


def require_value(label: str, cli_value: str | None, env_name: str) -> str:
    value = cli_value or os.environ.get(env_name)
    if value:
        return value.strip()
    raise SystemExit(f"Missing {label}. Provide --{label.replace('_', '-')} or set {env_name} in .env.")


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def chunk_date_range(start_date: date, end_date: date, max_days: int = MAX_WINDOW_DAYS) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    cursor = start_date
    max_delta = timedelta(days=max_days - 1)

    while cursor <= end_date:
        window_end = min(cursor + max_delta, end_date)
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)

    return windows


def slugify(value: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    collapsed = re.sub(r"-{2,}", "-", collapsed).strip("-._")
    return collapsed or "zoom-export"


def build_url(base_url: str, params: dict[str, Any] | None = None) -> str:
    if not params:
        return base_url

    filtered: list[tuple[str, str]] = []
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            filtered.append((key, "true" if value else "false"))
        else:
            filtered.append((key, str(value)))

    query = urlencode(filtered)
    return f"{base_url}?{query}" if query else base_url


def request_bytes(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    preserve_auth_on_redirect: bool = False,
) -> tuple[bytes, dict[str, str]]:
    opener = build_opener(TokenPreservingRedirectHandler()) if preserve_auth_on_redirect else build_opener()
    request = Request(url, data=data, headers=headers or {}, method=method)
    last_network_error: URLError | None = None

    for attempt in range(1, retries + 1):
        try:
            with opener.open(request, timeout=timeout) as response:
                response_body = response.read()
                response_headers = {key: value for key, value in response.headers.items()}
                return response_body, response_headers
        except HTTPError as exc:
            body = exc.read()
            if exc.code in {429, 500, 502, 503, 504} and attempt < retries:
                retry_after_header = exc.headers.get("Retry-After")
                delay_seconds = int(retry_after_header) if retry_after_header and retry_after_header.isdigit() else attempt * 2
                print_status(f"[warn] {method} {url} returned {exc.code}; retrying in {delay_seconds}s.")
                time.sleep(delay_seconds)
                continue
            raise ZoomApiError(url, exc.code, body) from exc
        except URLError as exc:
            last_network_error = exc
            if attempt < retries:
                delay_seconds = attempt * 2
                print_status(f"[warn] {method} {url} hit a network error; retrying in {delay_seconds}s.")
                time.sleep(delay_seconds)
                continue
            break

    raise ZoomApiError(url, None, str(last_network_error).encode("utf-8", errors="replace"))


def request_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> tuple[Any, dict[str, str]]:
    body, response_headers = request_bytes(
        url,
        method=method,
        headers=headers,
        data=data,
        retries=retries,
        timeout=timeout,
    )

    try:
        return json.loads(body.decode("utf-8")), response_headers
    except json.JSONDecodeError as exc:
        raise ZoomApiError(url, None, body[:200]) from exc


def zoom_api_json(
    token: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    method: str = "GET",
    data: bytes | None = None,
) -> Any:
    url = build_url(f"{API_BASE}{path}", params)
    payload, _headers = request_json(
        url,
        method=method,
        data=data,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    return payload


def request_access_token(account_id: str, client_id: str, client_secret: str) -> str:
    auth_blob = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    payload = urlencode(
        {
            "grant_type": "account_credentials",
            "account_id": account_id,
        }
    ).encode("utf-8")

    response_json, _headers = request_json(
        TOKEN_URL,
        method="POST",
        data=payload,
        headers={
            "Authorization": f"Basic {auth_blob}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )

    access_token = response_json.get("access_token")
    if not access_token:
        raise SystemExit("Zoom token response did not include access_token.")
    return str(access_token)


def encode_meeting_uuid(meeting_uuid: str) -> str:
    return quote(quote(meeting_uuid, safe=""), safe="")


def extract_recording_entries(page_payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("meetings", "recordings", "sessions"):
        value = page_payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def fetch_recordings_window(
    token: str,
    *,
    account_id: str,
    from_date: str,
    to_date: str,
    page_size: int,
    include_trash: bool,
    user_id: str | None,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    if user_id:
        candidate_paths = [f"/users/{quote(user_id, safe='')}/recordings"]
    else:
        candidate_paths = [
            f"/accounts/{quote(account_id, safe='')}/recordings",
            "/accounts/me/recordings",
        ]

    last_error: ZoomApiError | None = None

    for candidate_path in candidate_paths:
        pages: list[dict[str, Any]] = []
        entries: list[dict[str, Any]] = []
        next_page_token: str | None = None

        try:
            while True:
                page_payload = zoom_api_json(
                    token,
                    candidate_path,
                    params={
                        "from": from_date,
                        "to": to_date,
                        "page_size": min(max(page_size, 1), DEFAULT_PAGE_SIZE),
                        "next_page_token": next_page_token,
                        "trash": include_trash,
                    },
                )

                if not isinstance(page_payload, dict):
                    raise ZoomApiError(f"{API_BASE}{candidate_path}", None, repr(page_payload).encode("utf-8"))

                pages.append(page_payload)
                entries.extend(extract_recording_entries(page_payload))
                next_page_token = page_payload.get("next_page_token")
                if not next_page_token:
                    return candidate_path, pages, entries
        except ZoomApiError as exc:
            last_error = exc
            if exc.status_code in {400, 404} and candidate_path != candidate_paths[-1]:
                print_status(f"[warn] {candidate_path} failed with {exc.status_code}; trying alternate recordings endpoint.")
                continue
            raise

    if last_error is not None:
        raise last_error

    raise SystemExit("Could not find a working Zoom recordings endpoint.")


def hydrate_recording_files_if_needed(token: str, meeting: dict[str, Any]) -> dict[str, Any]:
    if meeting.get("recording_files"):
        return meeting

    meeting_uuid = str(meeting.get("uuid") or "").strip()
    meeting_id = str(meeting.get("id") or "").strip()
    if not meeting_uuid and not meeting_id:
        return meeting

    if meeting_uuid:
        path = f"/meetings/{encode_meeting_uuid(meeting_uuid)}/recordings"
    else:
        path = f"/meetings/{quote(meeting_id, safe='')}/recordings"

    detail_payload = zoom_api_json(token, path)
    if not isinstance(detail_payload, dict):
        return meeting

    merged = dict(meeting)
    for key, value in detail_payload.items():
        if key not in merged or key == "recording_files":
            merged[key] = value
    return merged


def meeting_matches(
    meeting: dict[str, Any],
    *,
    host_emails: set[str],
    topic_terms: list[str],
    topic_pattern: re.Pattern[str] | None,
    meeting_ids: set[str],
    exclude_meeting_ids: set[str],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    topic = str(meeting.get("topic") or "")
    topic_lower = topic.lower()
    host_email = str(meeting.get("host_email") or "").strip().lower()
    identifiers = {
        str(meeting.get("id") or "").strip(),
        str(meeting.get("uuid") or "").strip(),
    }
    identifiers.discard("")

    if exclude_meeting_ids and exclude_meeting_ids.intersection(identifiers):
        return False, []

    if meeting_ids:
        matched_ids = sorted(meeting_ids.intersection(identifiers))
        if not matched_ids:
            return False, []
        reasons.append(f"meeting_id={matched_ids[0]}")

    if host_emails:
        if host_email not in host_emails:
            return False, []
        reasons.append(f"host_email={host_email}")

    if topic_terms:
        matched_terms = [term for term in topic_terms if term in topic_lower]
        if not matched_terms:
            return False, []
        reasons.append(f"topic_contains={','.join(matched_terms)}")

    if topic_pattern:
        if not topic_pattern.search(topic):
            return False, []
        reasons.append(f"topic_regex={topic_pattern.pattern}")

    if not reasons:
        reasons.append("date_range_only")

    return True, reasons


def is_transcript_file(recording_file: dict[str, Any]) -> bool:
    file_type = str(recording_file.get("file_type") or "").upper()
    file_extension = str(recording_file.get("file_extension") or "").upper()
    recording_type = str(recording_file.get("recording_type") or "").lower()

    return (
        file_extension == "VTT"
        or file_type in TRANSCRIPT_FILE_TYPES
        or recording_type in TRANSCRIPT_RECORDING_TYPES
    )


def build_access_token_url(url: str, token: str) -> str:
    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_pairs = [(key, value) for key, value in query_pairs if key != "access_token"]
    query_pairs.append(("access_token", token))
    return urlunparse(parsed._replace(query=urlencode(query_pairs)))


def looks_like_vtt(payload: bytes) -> bool:
    trimmed = payload.lstrip()
    if trimmed.startswith(b"\xef\xbb\xbf"):
        trimmed = trimmed[3:]
    return trimmed.startswith(b"WEBVTT")


def sanitize_filename(value: str, *, max_length: int = 80) -> str:
    slug = slugify(value)
    if len(slug) > max_length:
        slug = slug[:max_length].rstrip("-._")
    return slug or "item"


def build_output_filename(meeting: dict[str, Any], recording_file: dict[str, Any]) -> str:
    meeting_start = (
        str(meeting.get("start_time") or "")
        or str(recording_file.get("recording_start") or "")
        or "unknown-time"
    )
    normalized_start = meeting_start.replace(":", "").replace("+00:00", "Z")
    meeting_id = str(meeting.get("id") or meeting.get("uuid") or "unknown-meeting")
    host_email = str(meeting.get("host_email") or "unknown-host").replace("@", "-at-")
    topic = str(meeting.get("topic") or "untitled")
    recording_type = str(recording_file.get("recording_type") or recording_file.get("file_type") or "transcript")
    file_id = str(recording_file.get("id") or "file")

    return "__".join(
        [
            sanitize_filename(normalized_start, max_length=32),
            sanitize_filename(meeting_id, max_length=48),
            sanitize_filename(host_email, max_length=48),
            sanitize_filename(topic, max_length=80),
            sanitize_filename(recording_type, max_length=32),
            sanitize_filename(file_id, max_length=48),
        ]
    ) + ".vtt"


def download_vtt(download_url: str, token: str, destination: Path, overwrite: bool) -> tuple[str, str]:
    if destination.exists() and destination.stat().st_size > 0 and not overwrite:
        return "skipped_existing", ""

    last_error_message = ""

    for label, candidate_url, headers, preserve_auth in [
        ("bearer_header", download_url, {"Authorization": f"Bearer {token}"}, True),
        ("query_token_fallback", build_access_token_url(download_url, token), {}, False),
    ]:
        try:
            payload, _response_headers = request_bytes(
                candidate_url,
                headers=headers,
                preserve_auth_on_redirect=preserve_auth,
            )
            if not looks_like_vtt(payload):
                snippet = payload[:160].decode("utf-8", errors="replace").strip()
                raise ZoomApiError(candidate_url, None, f"Expected WEBVTT payload, got: {snippet}".encode("utf-8"))

            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
            return ("downloaded" if label == "bearer_header" else "downloaded_with_query_token"), ""
        except ZoomApiError as exc:
            last_error_message = str(exc)
            print_status(f"[warn] Download attempt '{label}' failed for {download_url}: {exc}")

    return "failed", last_error_message


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "event_slug",
        "meeting_uuid",
        "meeting_id",
        "topic",
        "host_email",
        "start_time",
        "match_reasons",
        "file_id",
        "file_type",
        "file_extension",
        "recording_type",
        "download_url",
        "local_path",
        "download_status",
        "error",
    ]

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env")
    args = parse_args()

    start_date = parse_iso_date(args.from_date)
    end_date = parse_iso_date(args.to_date)
    if start_date > end_date:
        raise SystemExit("--from must be on or before --to.")

    account_id = require_value("account_id", args.account_id, "ZOOM_ACCOUNT_ID")
    client_id = require_value("client_id", args.client_id, "ZOOM_CLIENT_ID")
    client_secret = require_value("client_secret", args.client_secret, "ZOOM_CLIENT_SECRET")
    user_id = args.user_id or os.environ.get("ZOOM_USER_ID")

    event_slug = slugify(args.event_slug or f"zoom-export-{args.from_date}-to-{args.to_date}")
    data_root = (repo_root / args.data_root).resolve()
    export_root = data_root / event_slug
    metadata_root = export_root / "metadata"
    vtt_root = export_root / "vtt"

    host_emails = {value.strip().lower() for value in args.host_email if value and value.strip()}
    topic_terms = [value.strip().lower() for value in args.topic_contains if value and value.strip()]
    meeting_ids = {value.strip() for value in args.meeting_id if value and value.strip()}
    exclude_meeting_ids = {value.strip() for value in args.exclude_meeting_id if value and value.strip()}
    topic_pattern = re.compile(args.topic_regex, re.IGNORECASE) if args.topic_regex else None

    print_status("[info] Requesting Zoom access token...")
    token = request_access_token(account_id, client_id, client_secret)

    windows = chunk_date_range(start_date, end_date)
    all_pages: list[dict[str, Any]] = []
    all_recordings: list[dict[str, Any]] = []
    endpoint_used = ""

    for window_start, window_end in windows:
        print_status(f"[info] Fetching recordings for {window_start.isoformat()} to {window_end.isoformat()}...")
        endpoint_used, pages, entries = fetch_recordings_window(
            token,
            account_id=account_id,
            from_date=window_start.isoformat(),
            to_date=window_end.isoformat(),
            page_size=args.page_size,
            include_trash=args.include_trash,
            user_id=user_id,
        )
        all_pages.extend(pages)
        all_recordings.extend(entries)

    print_status(f"[info] Retrieved {len(all_recordings)} recordings via {endpoint_used}.")

    matched_rows: list[dict[str, Any]] = []
    matched_recording_count = 0
    download_failures = 0
    download_successes = 0
    download_skips = 0

    for original_meeting in all_recordings:
        is_match, match_reasons = meeting_matches(
            original_meeting,
            host_emails=host_emails,
            topic_terms=topic_terms,
            topic_pattern=topic_pattern,
            meeting_ids=meeting_ids,
            exclude_meeting_ids=exclude_meeting_ids,
        )
        if not is_match:
            continue

        meeting = hydrate_recording_files_if_needed(token, original_meeting)
        recording_files = meeting.get("recording_files")
        if not isinstance(recording_files, list):
            recording_files = []

        transcript_files = [item for item in recording_files if isinstance(item, dict) and is_transcript_file(item)]
        if not transcript_files:
            continue

        matched_recording_count += 1
        for recording_file in transcript_files:
            local_path = vtt_root / build_output_filename(meeting, recording_file)
            if args.dry_run:
                download_status = "dry_run"
                error = ""
            else:
                download_status, error = download_vtt(
                    str(recording_file.get("download_url") or ""),
                    token,
                    local_path,
                    overwrite=args.overwrite,
                )
                if download_status.startswith("downloaded"):
                    download_successes += 1
                elif download_status == "skipped_existing":
                    download_skips += 1
                elif download_status == "failed":
                    download_failures += 1

            matched_rows.append(
                {
                    "event_slug": event_slug,
                    "meeting_uuid": str(meeting.get("uuid") or ""),
                    "meeting_id": str(meeting.get("id") or ""),
                    "topic": str(meeting.get("topic") or ""),
                    "host_email": str(meeting.get("host_email") or ""),
                    "start_time": str(meeting.get("start_time") or ""),
                    "match_reasons": ";".join(match_reasons),
                    "file_id": str(recording_file.get("id") or ""),
                    "file_type": str(recording_file.get("file_type") or ""),
                    "file_extension": str(recording_file.get("file_extension") or ""),
                    "recording_type": str(recording_file.get("recording_type") or ""),
                    "download_url": str(recording_file.get("download_url") or ""),
                    "local_path": str(local_path),
                    "download_status": download_status,
                    "error": error,
                }
            )

    recordings_json_path = metadata_root / "recordings.json"
    matched_csv_path = metadata_root / "matched_transcripts.csv"
    summary_json_path = metadata_root / "summary.json"

    write_json(
        recordings_json_path,
        {
            "endpoint_used": endpoint_used,
            "filters": {
                "from": args.from_date,
                "to": args.to_date,
                "event_slug": event_slug,
                "user_id": user_id or "",
                "host_email": sorted(host_emails),
                "topic_contains": topic_terms,
                "topic_regex": args.topic_regex or "",
                "meeting_id": sorted(meeting_ids),
                "exclude_meeting_id": sorted(exclude_meeting_ids),
                "include_trash": args.include_trash,
            },
            "pages": all_pages,
        },
    )
    write_csv(matched_csv_path, matched_rows)
    write_json(
        summary_json_path,
        {
            "endpoint_used": endpoint_used,
            "recordings_seen": len(all_recordings),
            "recordings_with_transcripts_matched": matched_recording_count,
            "transcript_files_matched": len(matched_rows),
            "downloaded": download_successes,
            "skipped_existing": download_skips,
            "failed": download_failures,
            "dry_run": args.dry_run,
            "paths": {
                "recordings_json": str(recordings_json_path),
                "matched_csv": str(matched_csv_path),
                "vtt_root": str(vtt_root),
            },
        },
    )

    print(
        json.dumps(
            {
                "event_slug": event_slug,
                "recordings_seen": len(all_recordings),
                "recordings_with_transcripts_matched": matched_recording_count,
                "transcript_files_matched": len(matched_rows),
                "downloaded": download_successes,
                "skipped_existing": download_skips,
                "failed": download_failures,
                "dry_run": args.dry_run,
                "summary_json": str(summary_json_path),
                "matched_csv": str(matched_csv_path),
            },
            indent=2,
        )
    )

    return 1 if download_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
