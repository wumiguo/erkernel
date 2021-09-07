package org.wumiguo.erkernel.util

/**
 * @author levin 
 *         Created on 2021/9/7
 */
object GenericIO {
  def read(path: String): String = {
    if (path.startsWith("gs://")) {
      CloudStorageIO.readFromGCSPath(path)
    } else {
      val in = scala.io.Source.fromFile(path, "utf-8")
      try in.getLines.mkString finally in.close
    }
  }
}
