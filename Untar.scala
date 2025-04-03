import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import java.io.{BufferedInputStream, FileOutputStream}
import java.util.zip.GZIPInputStream
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

// Define storage paths
val storageFolderPath = "abfss://<container>@<storage-account>.dfs.core.windows.net/path/to/tarfiles/"
val extractPath = "abfss://<container>@<storage-account>.dfs.core.windows.net/path/to/extracted/"

// Get the Hadoop FileSystem
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

// List all .tar.gz files in the directory
val files: Array[FileStatus] = fs.listStatus(new Path(storageFolderPath))
val tarFiles = files.map(_.getPath.toString).filter(_.endsWith(".tar.gz"))

// Function to extract a single tar file
def extractTarFile(tarFilePath: String, destinationPath: String): Unit = {
    println(s"Extracting: $tarFilePath")

    // Open the Tar file
    val inputStream = fs.open(new Path(tarFilePath))
    val gzipInputStream = new GZIPInputStream(new BufferedInputStream(inputStream))
    val tarInputStream = new TarArchiveInputStream(gzipInputStream)

    var entry: TarArchiveEntry = tarInputStream.getNextTarEntry
    while (entry != null) {
        if (!entry.isDirectory) {
            val outputPath = new Path(destinationPath + entry.getName)
            val outputStream = fs.create(outputPath)

            val buffer = new Array 
            var bytesRead = tarInputStream.read(buffer)

            while (bytesRead != -1) {
                outputStream.write(buffer, 0, bytesRead)
                bytesRead = tarInputStream.read(buffer)
            }

            outputStream.close()
        }
        entry = tarInputStream.getNextTarEntry
    }

    // Close streams
    tarInputStream.close()
    gzipInputStream.close()
    inputStream.close()

    println(s"Extraction completed for $tarFilePath")
}

// Iterate over all tar files and extract them
tarFiles.foreach(filePath => extractTarFile(filePath, extractPath))

println("All tar files extracted successfully!")
