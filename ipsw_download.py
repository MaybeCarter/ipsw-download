import argparse
import concurrent.futures
import fnmatch
import hashlib
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Set

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

DEFAULT_STORAGE_GB = 200
DEFAULT_OUTPUT_DIR = "~/Library/Application Support/iTunes/iPhone Software Updates"
REQUEST_TIMEOUT = (10, 60)  # (connect, read) seconds
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MiB
ESTIMATED_SIZE_GB = 8.0
METADATA_FETCH_WORKERS = 8

# Matches IPSW filenames like "iPhone15,2_17.1_21B74_Restore.ipsw"
_IPSW_FILENAME_RE = re.compile(r"^(?P<identifier>[^_]+)_(?P<version>[^_]+)_")

log = logging.getLogger("ipsw_download")


class TqdmLoggingHandler(logging.Handler):
    """Logging handler that routes output through tqdm.write so progress bars aren't clobbered."""

    def emit(self, record):
        try:
            tqdm.write(self.format(record))
        except Exception:
            self.handleError(record)


def setup_logging(verbose: bool, quiet: bool) -> None:
    level = logging.DEBUG if verbose else (logging.WARNING if quiet else logging.INFO)
    log.setLevel(level)
    if not log.handlers:
        handler = TqdmLoggingHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(handler)
    log.propagate = False


@dataclass
class Candidate:
    device_name: str
    device_identifier: str
    version: str
    build_id: str
    url: str
    md5sum: Optional[str]
    sha1sum: Optional[str]
    size_bytes: Optional[int] = None
    already_present: bool = False

    @property
    def size_gb(self) -> float:
        if self.size_bytes is None:
            return ESTIMATED_SIZE_GB
        return self.size_bytes / (1000 ** 3)

    @property
    def filename(self) -> str:
        return os.path.basename(self.url)


def build_session() -> requests.Session:
    """Create a requests Session with retry/backoff defaults."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_file_size(session: requests.Session, url: str) -> Optional[int]:
    """Return the file size in bytes via a HEAD request, or None if unavailable."""
    try:
        response = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            return int(content_length)
        log.debug("Content-Length header not found for %s", url)
        return None
    except requests.RequestException as e:
        log.debug("Error fetching file size for %s: %s", url, e)
        return None


def _expected_hashes(md5sum: Optional[str], sha1sum: Optional[str]) -> dict:
    expected = {}
    if md5sum:
        expected["md5"] = md5sum.lower()
    if sha1sum:
        expected["sha1"] = sha1sum.lower()
    return expected


def _hash_file(path: str, algorithms: Iterable[str]) -> dict:
    hashers = {name: hashlib.new(name) for name in algorithms}
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(DOWNLOAD_CHUNK_SIZE), b""):
            for h in hashers.values():
                h.update(chunk)
    return {name: h.hexdigest().lower() for name, h in hashers.items()}


def existing_file_is_valid(filepath: str, md5sum: Optional[str], sha1sum: Optional[str]) -> bool:
    """Return True if filepath exists and matches every checksum the API provided."""
    if not os.path.exists(filepath):
        return False
    expected = _expected_hashes(md5sum, sha1sum)
    if not expected:
        return False
    log.info("Verifying %s...", os.path.basename(filepath))
    actual = _hash_file(filepath, expected.keys())
    return all(actual[alg] == digest for alg, digest in expected.items())


def _device_matches(identifier: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(identifier, pat) for pat in patterns)


def _filter_models(models: List[dict], include: List[str], exclude: List[str]) -> List[dict]:
    out = []
    for m in models:
        identifier = m["identifier"]
        if include and not _device_matches(identifier, include):
            continue
        if exclude and _device_matches(identifier, exclude):
            continue
        out.append(m)
    return out


def _fetch_candidates_for_device(session, model: dict, keep: int) -> List[Candidate]:
    device_identifier = model["identifier"]
    try:
        response = session.get(
            f"https://api.ipsw.me/v4/device/{device_identifier}?type=ipsw",
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        log.warning(
            "Failed to fetch firmware metadata for %s (%s): %s",
            model["name"], device_identifier, e,
        )
        return []

    firmwares = data.get("firmwares") or []
    if not firmwares:
        log.info("No IPSW available for %s (%s)", model["name"], device_identifier)
        return []

    candidates = []
    for firmware in firmwares[:keep]:
        candidates.append(
            Candidate(
                device_name=model["name"],
                device_identifier=device_identifier,
                version=firmware["version"],
                build_id=firmware["buildid"],
                url=firmware["url"],
                md5sum=firmware.get("md5sum"),
                sha1sum=firmware.get("sha1sum"),
            )
        )
    return candidates


def gather_candidates(session, models: List[dict], keep: int) -> List[Candidate]:
    """Fetch firmware metadata and file sizes for every model in parallel."""
    if not models:
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=METADATA_FETCH_WORKERS) as executor:
        per_device = list(
            executor.map(lambda m: _fetch_candidates_for_device(session, m, keep), models)
        )
    flat = [c for group in per_device for c in group]

    def _populate_size(c: Candidate) -> Candidate:
        c.size_bytes = get_file_size(session, c.url)
        return c

    with concurrent.futures.ThreadPoolExecutor(max_workers=METADATA_FETCH_WORKERS) as executor:
        flat = list(executor.map(_populate_size, flat))
    return flat


def apply_storage_budget(
    candidates: List[Candidate], max_storage_gb: float, output_dir: str
) -> List[Candidate]:
    """Greedily pick candidates (in input order) that fit within the storage budget."""
    selected: List[Candidate] = []
    total_new_gb = 0.0
    log.info("Devices under consideration:\n")
    for c in candidates:
        size_note = "" if c.size_bytes is not None else " (estimated)"
        log.info(
            "Device: %s (%s), iOS Version: %s, Build ID: %s, Size: %.2f GB%s",
            c.device_name, c.device_identifier, c.version, c.build_id, c.size_gb, size_note,
        )
        filepath = os.path.join(output_dir, c.filename)
        c.already_present = existing_file_is_valid(filepath, c.md5sum, c.sha1sum)
        new_cost = 0.0 if c.already_present else c.size_gb
        if total_new_gb + new_cost > max_storage_gb:
            log.info("  Skipping — would exceed storage budget (%.2f GB).", max_storage_gb)
            continue
        selected.append(c)
        total_new_gb += new_cost
    log.info("\nTotal estimated new download size: %.2f GB\n", total_new_gb)
    return selected


def clean_old_files(device_identifier: str, keep_versions: Set[str], output_dir: str) -> None:
    """Delete IPSW files for this device whose version isn't in keep_versions."""
    for entry in os.listdir(output_dir):
        if not entry.endswith(".ipsw"):
            continue
        match = _IPSW_FILENAME_RE.match(entry)
        if not match:
            continue
        if match.group("identifier") != device_identifier:
            continue
        if match.group("version") in keep_versions:
            continue
        path = os.path.join(output_dir, entry)
        try:
            os.remove(path)
            log.info("Deleted outdated IPSW for %s: %s", device_identifier, entry)
        except OSError as e:
            log.warning("Failed to delete %s: %s", path, e)


def download(session, candidate: Candidate, output_dir: str) -> bool:
    """Download and verify an IPSW. Returns True on success."""
    filepath = os.path.join(output_dir, candidate.filename)
    part_path = filepath + ".part"

    if candidate.already_present:
        log.info("%s already exists with correct hash, skipping.", candidate.filename)
        return True

    if os.path.exists(filepath):
        log.info("Incorrect file for %s, re-downloading.", candidate.filename)
        os.remove(filepath)
    if os.path.exists(part_path):
        os.remove(part_path)

    log.info(
        "Starting download for %s (%s), Version: %s, URL: %s",
        candidate.device_name, candidate.device_identifier, candidate.version, candidate.url,
    )
    try:
        response = session.get(candidate.url, stream=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        log.warning("Download failed for %s: %s", candidate.filename, e)
        return False

    content_length = response.headers.get("Content-Length")
    total_size = int(content_length) if content_length is not None else None

    expected = _expected_hashes(candidate.md5sum, candidate.sha1sum)
    if not expected:
        log.warning(
            "No checksum available from the API for %s; downloading without verification.",
            candidate.filename,
        )
    hashers = {name: hashlib.new(name) for name in expected.keys()}

    try:
        with open(part_path, "wb") as fh, tqdm(
            total=total_size, unit="B", unit_scale=True, desc=candidate.filename
        ) as pbar:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
                    for h in hashers.values():
                        h.update(chunk)
                    pbar.update(len(chunk))
    except (requests.RequestException, OSError) as e:
        log.warning("Download failed for %s: %s", candidate.filename, e)
        if os.path.exists(part_path):
            os.remove(part_path)
        return False

    for alg, expected_digest in expected.items():
        actual = hashers[alg].hexdigest().lower()
        if actual != expected_digest:
            log.warning(
                "%s mismatch for %s (expected %s, got %s). Removing corrupt file.",
                alg.upper(), candidate.filename, expected_digest, actual,
            )
            os.remove(part_path)
            return False

    os.replace(part_path, filepath)
    log.info("Download complete: %s", candidate.filename)
    return True


def _iphone_sort_key(device: dict):
    """Sort iPhones by (major, minor) from their identifier; unknowns go last."""
    match = re.match(r"^iPhone(\d+),(\d+)$", device["identifier"])
    if not match:
        return (-1, -1)
    return (int(match.group(1)), int(match.group(2)))


def run(args) -> int:
    output_directory = os.path.expanduser(args.output)
    os.makedirs(output_directory, exist_ok=True)

    session = build_session()

    try:
        response = session.get("https://api.ipsw.me/v4/devices", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        log.error("Error fetching device list: %s", e)
        return 1

    iphones = [device for device in data if device["identifier"].startswith("iPhone")]
    iphones = _filter_models(iphones, args.device, args.exclude)
    if not iphones:
        log.warning("No iPhone devices matched the filters.")
        return 2
    iphones.sort(key=_iphone_sort_key, reverse=True)

    log.info("Fetching firmware metadata for %d device(s)...", len(iphones))
    candidates = gather_candidates(session, iphones, args.keep)
    if not candidates:
        log.warning("No firmware candidates found.")
        return 2

    selected = apply_storage_budget(candidates, args.storage, output_directory)
    if not selected:
        log.warning("No devices fit within the storage budget.")
        return 2

    if args.dry_run:
        log.info("Dry run: would download %d IPSW(s):", len(selected))
        for c in selected:
            marker = " [already present]" if c.already_present else ""
            log.info(
                "  %s (%s) v%s [%s] - %.2f GB%s",
                c.device_name, c.device_identifier, c.version, c.build_id, c.size_gb, marker,
            )
        return 0

    log.info("Starting downloads...\n")
    failed_devices: Set[str] = set()
    succeeded_count = 0
    for c in selected:
        if download(session, c, output_directory):
            succeeded_count += 1
        else:
            failed_devices.add(c.device_identifier)

    # For cleanup, preserve the full top-N keep set from the candidates (not just the
    # selected subset) so pre-existing files for top-N versions aren't wiped just because
    # they didn't need to be re-downloaded. Only touch devices where every selected
    # download succeeded, to avoid nuking fallbacks when the latest download failed.
    keep_versions_per_device: dict = {}
    for c in candidates:
        keep_versions_per_device.setdefault(c.device_identifier, set()).add(c.version)

    attempted_device_ids = {c.device_identifier for c in selected}
    for device_id in attempted_device_ids - failed_devices:
        clean_old_files(device_id, keep_versions_per_device[device_id], output_directory)

    if failed_devices:
        log.warning(
            "%d download(s) failed across %d device(s).",
            len(selected) - succeeded_count, len(failed_devices),
        )
        return 1
    return 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Download latest iPhone IPSW files up to a configurable storage limit."
    )
    parser.add_argument(
        "-s", "--storage", type=float, default=DEFAULT_STORAGE_GB,
        help=f"Maximum storage space to fill, in GB (default: {DEFAULT_STORAGE_GB}).",
    )
    parser.add_argument(
        "-o", "--output", default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write IPSWs into (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "-d", "--device", action="append", default=[], metavar="PATTERN",
        help=(
            "Only consider devices whose identifier matches this fnmatch glob "
            "(e.g. 'iPhone15,*' or 'iPhone15,2'). Can be given multiple times."
        ),
    )
    parser.add_argument(
        "-x", "--exclude", action="append", default=[], metavar="PATTERN",
        help="Exclude devices whose identifier matches this fnmatch glob. Can be given multiple times.",
    )
    parser.add_argument(
        "-k", "--keep", type=int, default=1, metavar="N",
        help="How many recent firmware versions to retain per device (default: 1).",
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true",
        help="Plan what would be downloaded without actually downloading.",
    )
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging.")
    verbosity.add_argument("-q", "--quiet", action="store_true", help="Suppress informational output.")

    args = parser.parse_args(argv)
    if args.storage <= 0:
        parser.error("--storage must be greater than 0")
    if args.keep < 1:
        parser.error("--keep must be at least 1")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose, args.quiet)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
