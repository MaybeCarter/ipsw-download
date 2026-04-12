import argparse
import os
import requests
import hashlib
from tqdm import tqdm

def get_file_size(url):
“”“Attempt to retrieve file size from the HTTP headers using a HEAD request.”””
try:
response = requests.head(url, allow_redirects=True)
if ‘Content-Length’ in response.headers:
return int(response.headers[‘Content-Length’])
else:
print(“Content-Length header not found, using estimated size.”)
return None
except Exception as e:
print(f”Error fetching file size: {e}”)
return None

def pre_check_ipsw_sizes(recent_models, max_storage_gb):
“”“Check IPSW sizes for each device and prepare download list based on storage limit.”””
total_storage_gb = 0
download_list = []

```
print("Devices selected for download (based on storage limit):\n")

for model in recent_models:
   device_identifier = model["identifier"]
   firmware_response = requests.get(f"https://api.ipsw.me/v4/device/{device_identifier}?type=ipsw")
   firmware_response.raise_for_status()
   firmware_data = firmware_response.json()

   if not firmware_data["firmwares"]:
       print(f"No IPSW available for {model['name']} ({device_identifier})\n")
       continue

   latest_firmware = firmware_data["firmwares"][0]
   download_url = latest_firmware["url"]
   version = latest_firmware["version"]
   build_id = latest_firmware["buildid"]
   md5sum = latest_firmware["md5sum"]

   size_bytes = get_file_size(download_url)
   if size_bytes is not None:
       size_gb = size_bytes / (1000 ** 3)  # Convert bytes to gigabytes (GB)
   else:
       size_gb = 8.0  # Estimated 8GB size for recent models
       print(f"Warning: Missing size information for {model['name']} ({device_identifier}). Using estimated size of {size_gb} GB.\n")

   # Print each device's info as soon as its size is determined
   print(f"Device: {model['name']} ({device_identifier}), iOS Version: {version}, Build ID: {build_id}, Size: {size_gb:.2f} GB")

   if total_storage_gb + size_gb > max_storage_gb:
       break

   download_list.append((model['name'], device_identifier, version, build_id, download_url, md5sum, size_gb))
   total_storage_gb += size_gb

print(f"\nTotal estimated download size: {total_storage_gb:.2f} GB\n")
return download_list
```

def clean_old_files(device_name, device_identifier, new_version, output_dir):
“”“Delete outdated IPSW files after new download completes.”””
for file in os.listdir(output_dir):
if device_identifier in file and not new_version in file:
os.remove(os.path.join(output_dir, file))
print(f”Deleted outdated IPSW for {device_name} ({device_identifier}): {file} - Replaced with updated version: {new_version}.”)

def download(url, expected_md5, output_dir, size_gb, device_name, device_identifier, version):
“”“Download and verify MD5 hash of IPSW file with progress bar.”””
if not url.endswith(’.ipsw’):
print(“URL does not point to an IPSW file.”)
return

```
filename = os.path.basename(url)
filepath = os.path.join(output_dir, filename)

if os.path.exists(filepath):
   print(f"Verifying {filename}...")
   verify_md5 = hashlib.md5()
   with open(filepath, 'rb') as file:
       for chunk in iter(lambda: file.read(4096), b""):
           verify_md5.update(chunk)
   if verify_md5.hexdigest().lower() == expected_md5.lower():
       print(f"{filename} already exists with correct hash, skipping.\n")
       return
   else:
       print(f"Incomplete or incorrect download for {filename}, re-downloading.")
       os.remove(filepath)

print(f"Starting download for {device_name} ({device_identifier}), Version: {version}, URL: {url}")
response = requests.get(url, stream=True)
response.raise_for_status()

hash_md5 = hashlib.md5()
total_size = int(size_gb * 1000 ** 3)  # Convert GB to bytes for tqdm
with open(filepath, 'wb') as file, tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
   for chunk in response.iter_content(chunk_size=1024):
       if chunk:
           file.write(chunk)
           hash_md5.update(chunk)
           pbar.update(len(chunk))

if hash_md5.hexdigest().lower() != expected_md5.lower():
   print(f"MD5 mismatch for {filename}. Removing corrupt file.")
   os.remove(filepath)
else:
   print(f"Download complete: {filename}\n")
   # Perform cleanup of outdated IPSW after successful download
   clean_old_files(device_name, device_identifier, version, output_dir)
```

def fetch_latest_ipsws(output_dir, max_storage_gb=200):
“”“Fetch the latest IPSWs for iPhone models using the V4 API and limit by storage.”””
try:
response = requests.get(“https://api.ipsw.me/v4/devices”)
response.raise_for_status()
data = response.json()

```
   iphones = [device for device in data if device["identifier"].startswith("iPhone")]
   recent_models = sorted(
       iphones,
       key=lambda x: int(x["identifier"].split(",")[0].replace("iPhone", "")),
       reverse=True
   )

   # Pre-check to calculate total size and print expected downloads
   download_list = pre_check_ipsw_sizes(recent_models, max_storage_gb)

   # Start actual downloads
   print("\nStarting downloads...\n")
   for item in download_list:
       download(item[4], item[5], output_dir, item[6], item[0], item[1], item[2])

except Exception as e:
   print(f"Error fetching firmware data: {e}")
   return
```

# Dynamically set the output directory to the iTunes iPhone Software Updates folder

output_directory = os.path.expanduser(”~/Library/Application Support/iTunes/iPhone Software Updates”)
os.makedirs(output_directory, exist_ok=True)

# Run the download function with a configurable storage limit

parser = argparse.ArgumentParser(
    description="Download latest iPhone IPSW files up to a configurable storage limit."
)
parser.add_argument(
    "-s",
    "--storage",
    type=float,
    default=200,
    help="Maximum storage space to fill, in GB (default: 200).",
)
args = parser.parse_args()

fetch_latest_ipsws(output_directory, max_storage_gb=args.storage)