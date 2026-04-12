import argparse
import hashlib
import os
import re
import sys

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

DEFAULT_STORAGE_GB = 200
DEFAULT_OUTPUT_DIR = "~/Library/Application Support/iTunes/iPhone Software Updates"
REQUEST_TIMEOUT = (10, 60)  # (connect, read) seconds
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MiB
ESTIMATED_SIZE_GB = 8.0

# Matches IPSW filenames like "iPhone15,2_17.1_21B74_Restore.ipsw"
_IPSW_FILENAME_RE = re.compile(r"^(?P<identifier>[^_]+)_(?P<version>[^_]+)_")


def build_session():
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


def get_file_size(session, url):
    """Return the file size in bytes via a HEAD request, or None if unavailable."""
    try:
        response = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            return int(content_length)
        print("Content-Length header not found, using estimated size.")
        return None
    except requests.RequestException as e:
        print(f"Error fetching file size: {e}")
        return None


def md5_of_file(path):
    """Compute the MD5 hex digest of a file on disk."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(DOWNLOAD_CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def existing_file_is_valid(filepath, expected_md5):
    """Return True if filepath exists and its MD5 matches expected_md5."""
    if not os.path.exists(filepath):
        return False
    print(f"Verifying {os.path.basename(filepath)}...")
    return md5_of_file(filepath) == expected_md5.lower()


def pre_check_ipsw_sizes(session, recent_models, max_storage_gb, output_dir):
    """Check IPSW sizes for each device and prepare download list based on storage limit."""
    total_new_storage_gb = 0.0
    download_list = []

    print("Devices under consideration:\n")

    for model in recent_models:
        device_identifier = model["identifier"]
        try:
            firmware_response = session.get(
                f"https://api.ipsw.me/v4/device/{device_identifier}?type=ipsw",
                timeout=REQUEST_TIMEOUT,
            )
            firmware_response.raise_for_status()
            firmware_data = firmware_response.json()
        except requests.RequestException as e:
            print(
                f"Failed to fetch firmware metadata for {model['name']} "
                f"({device_identifier}): {e}"
            )
            continue

        if not firmware_data.get("firmwares"):
            print(f"No IPSW available for {model['name']} ({device_identifier})\n")
            continue

        latest_firmware = firmware_data["firmwares"][0]
        download_url = latest_firmware["url"]
        version = latest_firmware["version"]
        build_id = latest_firmware["buildid"]
        md5sum = latest_firmware["md5sum"]

        size_bytes = get_file_size(session, download_url)
        if size_bytes is not None:
            size_gb = size_bytes / (1000 ** 3)
        else:
            size_gb = ESTIMATED_SIZE_GB
            print(
                f"Warning: Missing size information for {model['name']} "
                f"({device_identifier}). Using estimated size of {size_gb} GB.\n"
            )

        print(
            f"Device: {model['name']} ({device_identifier}), iOS Version: {version}, "
            f"Build ID: {build_id}, Size: {size_gb:.2f} GB"
        )

        # A file already on disk with the right hash costs zero new space.
        filename = os.path.basename(download_url)
        filepath = os.path.join(output_dir, filename)
        already_present = existing_file_is_valid(filepath, md5sum)
        new_cost_gb = 0.0 if already_present else size_gb

        if total_new_storage_gb + new_cost_gb > max_storage_gb:
            print(
                f"  Stopping — adding this device would exceed the storage budget "
                f"({max_storage_gb} GB)."
            )
            break

        download_list.append(
            (model["name"], device_identifier, version, build_id, download_url, md5sum)
        )
        total_new_storage_gb += new_cost_gb

    print(f"\nTotal estimated new download size: {total_new_storage_gb:.2f} GB\n")
    return download_list


def clean_old_files(device_identifier, new_version, output_dir):
    """Delete outdated IPSW files for this device after a successful new download."""
    for entry in os.listdir(output_dir):
        if not entry.endswith(".ipsw"):
            continue
        match = _IPSW_FILENAME_RE.match(entry)
        if not match:
            continue
        if match.group("identifier") != device_identifier:
            continue
        if match.group("version") == new_version:
            continue
        path = os.path.join(output_dir, entry)
        try:
            os.remove(path)
            print(
                f"Deleted outdated IPSW for {device_identifier}: {entry} - "
                f"Replaced with updated version: {new_version}."
            )
        except OSError as e:
            print(f"Warning: failed to delete {path}: {e}")


def download(session, url, expected_md5, output_dir, device_name, device_identifier, version):
    """Download and verify the MD5 hash of an IPSW file with a progress bar."""
    filename = os.path.basename(url)
    filepath = os.path.join(output_dir, filename)
    part_path = filepath + ".part"

    if existing_file_is_valid(filepath, expected_md5):
        print(f"{filename} already exists with correct hash, skipping.\n")
        return True

    if os.path.exists(filepath):
        print(f"Incorrect file for {filename}, re-downloading.")
        os.remove(filepath)
    if os.path.exists(part_path):
        os.remove(part_path)

    print(
        f"Starting download for {device_name} ({device_identifier}), "
        f"Version: {version}, URL: {url}"
    )
    try:
        response = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Download failed for {filename}: {e}")
        return False

    content_length = response.headers.get("Content-Length")
    total_size = int(content_length) if content_length is not None else None

    hash_md5 = hashlib.md5()
    try:
        with open(part_path, "wb") as fh, tqdm(
            total=total_size, unit="B", unit_scale=True, desc=filename
        ) as pbar:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
                    hash_md5.update(chunk)
                    pbar.update(len(chunk))
    except (requests.RequestException, OSError) as e:
        print(f"Download failed for {filename}: {e}")
        if os.path.exists(part_path):
            os.remove(part_path)
        return False

    if hash_md5.hexdigest().lower() != expected_md5.lower():
        print(f"MD5 mismatch for {filename}. Removing corrupt file.")
        os.remove(part_path)
        return False

    os.replace(part_path, filepath)
    print(f"Download complete: {filename}\n")
    clean_old_files(device_identifier, version, output_dir)
    return True


def _iphone_sort_key(device):
    """Sort iPhones by (major, minor) from their identifier; unknowns go last."""
    match = re.match(r"^iPhone(\d+),(\d+)$", device["identifier"])
    if not match:
        return (-1, -1)
    return (int(match.group(1)), int(match.group(2)))


def fetch_latest_ipsws(output_dir, max_storage_gb):
    """Fetch the latest IPSWs for iPhone models using the V4 API and limit by storage."""
    session = build_session()
    try:
        response = session.get("https://api.ipsw.me/v4/devices", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching device list: {e}")
        return 1

    iphones = [device for device in data if device["identifier"].startswith("iPhone")]
    recent_models = sorted(iphones, key=_iphone_sort_key, reverse=True)

    download_list = pre_check_ipsw_sizes(session, recent_models, max_storage_gb, output_dir)

    if not download_list:
        print("No devices fit within the storage budget.")
        return 2

    print("\nStarting downloads...\n")
    failures = 0
    for name, identifier, version, _build_id, url, md5sum in download_list:
        if not download(session, url, md5sum, output_dir, name, identifier, version):
            failures += 1

    if failures:
        print(f"{failures} download(s) failed.")
        return 1
    return 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Download latest iPhone IPSW files up to a configurable storage limit."
    )
    parser.add_argument(
        "-s",
        "--storage",
        type=float,
        default=DEFAULT_STORAGE_GB,
        help=f"Maximum storage space to fill, in GB (default: {DEFAULT_STORAGE_GB}).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write IPSWs into (default: {DEFAULT_OUTPUT_DIR}).",
    )
    args = parser.parse_args(argv)
    if args.storage <= 0:
        parser.error("--storage must be greater than 0")
    return args


def main(argv=None):
    args = parse_args(argv)
    output_directory = os.path.expanduser(args.output)
    os.makedirs(output_directory, exist_ok=True)
    return fetch_latest_ipsws(output_directory, max_storage_gb=args.storage)


if __name__ == "__main__":
    sys.exit(main())
