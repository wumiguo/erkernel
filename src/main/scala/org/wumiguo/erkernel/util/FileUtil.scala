package org.wumiguo.erkernel.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object FileUtil {
  private lazy implicit val spark: SparkSession = SparkSession.builder().getOrCreate();

  //shared fs API will surely have issue when working with GCP CloudsStorage.
  //e.g. when work with 'gs://' the bucket link, it will throw error like 'IllegalArgumentException: Wrong FS: gs://demo-bucket/sparkjobconfig/app.json, excepted: hdfs://dataproc01-master
  //private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  private val log = LoggerFactory.getLogger(this.getClass.getName)

  def eraseHdfsDir(dir: String)(implicit spark: SparkSession): Unit = {
    val targetPath = new Path(dir)
    //instead create fs from path object which is a more safer solution to word with gcs
    val fs = targetPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(targetPath)) {
      fs.delete(targetPath, true)
      log.info("delete existing dir [" + dir + "]")
    }
    log.info("create dir[" + dir + "]")
    fs.mkdirs(targetPath)
  }

  def exist(path: String): Boolean = {
    val target = new Path(path)
    val fs = target.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(target)
  }

  def list(path: String): Seq[Path] = {
    val files = ListBuffer[Path]()
    val targetPath = new Path(path)
    val fs = targetPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val it = fs.listFiles(targetPath, true)
    while (it.hasNext) {
      files += it.next().getPath
    }
    files
  }

}
