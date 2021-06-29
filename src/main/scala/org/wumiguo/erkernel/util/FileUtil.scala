package org.wumiguo.erkernel.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object FileUtil {
  private lazy implicit val spark: SparkSession = SparkSession.builder().getOrCreate();
  private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val log = LoggerFactory.getLogger(this.getClass.getName)

  def eraseHdfsDir(dir: String)(implicit spark: SparkSession): Unit = {
    val targetPath = new Path(dir)
    if (fs.exists(targetPath)) {
      fs.delete(targetPath, true)
      log.info("delete existing dir [" + dir + "]")
    }
    log.info("create dir[" + dir + "]")
    fs.mkdirs(targetPath)
  }
}
