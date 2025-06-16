package redis.utils

import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths

object File:
  def getStream(filePath: String): Option[InputStream] =
    val path = Paths.get(filePath)

    if Files.exists(path) && Files.isRegularFile(path)
    then Some(new FileInputStream(path.toFile))
    else None
