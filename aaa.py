from py4j.java_gateway import java_import
import tarfile
import os

# Set up paths
adls_path = "abfss://<container>@<storageaccount>.dfs.core.windows.net/path/to/file.tar"
local_tar_path = "/tmp/archive.tar"
extract_path = "/tmp/extracted"

# Create extract folder
os.makedirs(extract_path, exist_ok=True)

# Use Hadoop FileSystem API to copy the .tar file locally
java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")

fs = sc._jvm.FileSystem.get(sc._jsc.hadoopConfiguration())
src_path = sc._jvm.Path(adls_path)
dst_path = sc._jvm.Path(f"file:{local_tar_path}")

fs.copyToLocalFile(src_path, dst_path)


# Now extract the contents
if os.path.exists(local_tar_path):
    with tarfile.open(local_tar_path, "r") as tar:
        tar.extractall(path=extract_path)
    print("✅ Extraction completed.")
else:
    print("❌ File not found:", local_tar_path)
