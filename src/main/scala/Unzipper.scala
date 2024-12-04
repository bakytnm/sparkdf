import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

object Unzipper {
  def run(files: String, outputPath: String): Unit ={
    val zipDir = new File(files)
    val folder = new File(outputPath)
    if (!folder.exists()) folder.mkdir

    zipDir.listFiles().filter(_.getName.endsWith(".zip")).foreach(file => {
      unzip(file.getAbsolutePath, outputPath)
    })
  }

  private def unzip(zipFile: String, outputFolder: String): Unit = {
    val zipStr: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile));
    var zipEntry: ZipEntry = zipStr.getNextEntry;
    val buffer = new Array[Byte](4096)

    while (zipEntry != null) {
      val fileName = zipEntry.getName
      val newFile = new File(outputFolder + File.separator + fileName)
      new File(newFile.getParent).mkdirs()

      if (!zipEntry.isDirectory) {
        val fos = new FileOutputStream(newFile)
        var len: Int = zipStr.read(buffer)

        println("file to unzip : " + newFile.getAbsoluteFile)
        while (len > 0) {
          fos.write(buffer, 0, len)
          len = zipStr.read(buffer)
        }
        fos.close()
      }
      zipEntry = zipStr.getNextEntry
    }

    zipStr.closeEntry()
    zipStr.close()
  }
}
