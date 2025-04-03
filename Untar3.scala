import java.io.{BufferedInputStream, FileOutputStream}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}

// Define paths
val storageFolderPath = "abfss://<container>@<storage-account>.dfs.core.windows.net/path/to/tarfiles/"
val extractPath = "abfss://<container>@<storage-account>.dfs.core.windows.net/path/to/extracted/"

// List .tar files using Spark DataFrame
val tarFilesDF = spark.read.format("binaryFile").load(storageFolderPath)
val tarFiles = tarFilesDF.select("path").as[String].collect().filter(_.endsWith(".tar"))

if (tarFiles.isEmpty) {
    println("No .tar files found!")
} else {
    tarFiles.foreach { tarFilePath =>
        println(s"Extracting: $tarFilePath")

        // Read tar file as binary stream
        val binaryDF = spark.read.format("binaryFile").load(tarFilePath)
        val inputStream = new BufferedInputStream(binaryDF.head().getAs .inputStream)
        val tarInputStream = new TarArchiveInputStream(inputStream)

        var entry: TarArchiveEntry = tarInputStream.getNextTarEntry
        while (entry != null) {
            if (!entry.isDirectory) {
                val outputPath = extractPath + entry.getName
                val outputStream = new FileOutputStream(outputPath)

                val buffer = new Array 
                var bytesRead = tarInputStream.read(buffer)

                while (bytesRead != -1) {
                    outputStream.write(buffer, 0, bytesRead)
                    bytesRead = tarInputStream.read(buffer)
                }

                outputStream.close()
                println(s"Extracted file: ${entry.getName}")
            }
            entry = tarInputStream.getNextTarEntry
        }

        tarInputStream.close()
        inputStream.close()

        println(s"Finished extracting: $tarFilePath")
    }

    println("All .tar files extracted successfully!")
}
