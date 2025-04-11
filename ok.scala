# Define your path
tar_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/path/to/yourfile.tar"
local_tar_path = "/tmp/archive.tar"

# Copy tar to Spark driver local disk
dbutils.fs.cp(tar_path, f"file:{local_tar_path}")

import tarfile
import os

extract_path = "/tmp/untarred/"
os.makedirs(extract_path, exist_ok=True)

# Extract files
with tarfile.open(local_tar_path, "r") as tar:
    tar.extractall(path=extract_path)

# Upload files back to ADLS
extracted_files = os.listdir(extract_path)
for filename in extracted_files:
    src = f"file:{extract_path}/{filename}"
    dest = f"abfss://<container>@<storage-account>.dfs.core.windows.net/extracted/{filename}"
    dbutils.fs.cp(src, dest)
