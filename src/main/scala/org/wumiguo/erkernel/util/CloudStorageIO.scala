package org.wumiguo.erkernel.util

import java.io.ByteArrayOutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession

/**
 * @author levin 
 *         Created on 2021/9/7
 */
object CloudStorageIO {
  private lazy implicit val spark: SparkSession = SparkSession.builder().getOrCreate();

  def readFromGCSPath(path: String): String = {
    val gcsFs = new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val in = gcsFs.open(new Path(path))
    try {
      val out = new ByteArrayOutputStream()
      IOUtils.copyBytes(in, out, 4096, false)
      new String(out.toByteArray)
    } finally {
      IOUtils.closeStream(in)
    }
  }
}
