import os
import tarfile

# Set paths
remote_path = "abfss://<container>@<account>.dfs.core.windows.net/path/to/yourfile.tar"
local_tar_path = "/tmp/archive.tar"
extract_path = "/tmp/extracted"

# Create local extract dir
os.makedirs(extract_path, exist_ok=True)

# Copy file from ADLS to local /tmp (works in Synapse)
spark.conf.set("fs.azure.account.key.<account>.dfs.core.windows.net", "<your_key_if_needed>")
dbutils.fs.cp(remote_path, f"file:{local_tar_path}")  # Make sure this step succeeds!

# ✅ Check if the file exists before opening
if os.path.exists(local_tar_path):
    with tarfile.open(local_tar_path, "r") as tar:
        tar.extractall(path=extract_path)
    print("✅ Extraction done")
else:
    print("❌ Tar file not found at", local_tar_path)
