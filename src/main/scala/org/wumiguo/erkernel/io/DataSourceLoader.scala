package org.wumiguo.erkernel.io

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.common.Loggable
import org.wumiguo.erkernel.model.{DataSource, DataSourceDict, DataSourcePath}
import org.wumiguo.erkernel.util.JsonReader

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object DataSourceLoader extends Loggable {
  private val PARQUET_EXT_LEN = ".parq".length

  def extractDataSourceName(path: String): String = {
    path.split("/").last.dropRight(PARQUET_EXT_LEN)
  }

  def getLastRunPath(parquetFolder: String, partitionBy: String = "run_date")(implicit spark: SparkSession): Seq[String] = {
    val path = new Path(parquetFolder)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val theRuns: Array[String] = try {
      Array(fs.listStatus(path)
        .map(file => file.getPath)
        .filter(file => file.getName.contains(partitionBy + "="))
        .map(file => file.toString)
        .sorted
        .last)
    }
    LOG.info(s"theRuns=${theRuns.toSeq}")
    val lastRuns: Array[String] = if (!theRuns.isEmpty) {
      theRuns
    } else {
      Array(path.toString)
    }
    lastRuns
  }

  def getLastDataSourcePaths(dataSourceDataDir: String, dataSourceDict: DataSourceDict)(implicit spark: SparkSession): Map[String, DataSourcePath] = {
    val path = new Path(dataSourceDataDir)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val rawParquetPaths = fs.listStatus(path)
      .filter(_.isDirectory)
      .filter(_.toString.contains(".parq")) //collection *.parq or *.parquet folder
      .map(_.getPath.toString)
    LOG.info(s"rawParquetPaths=${rawParquetPaths.toSeq}")
    val allParquetPaths = rawParquetPaths.distinct
    val parqPathsBySource = allParquetPaths
      .map {
        path => (extractDataSourceName(path), path)
      }
      .filter(entry => dataSourceDict.dict.keySet.contains(entry._1))
    parqPathsBySource.map(entry => (entry._1, DataSourcePath(entry._2, getLastRunPath(entry._2)))).toMap
  }


}
