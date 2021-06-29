package org.wumiguo.erkernel.util

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object CSV2ParquetFilesUtil {
  private val CSV_EXT_LEN = ".csv".length

  private val log = LoggerFactory.getLogger(this.getClass.getName)

  private def subFiles(dir: File): Seq[File] = dir.listFiles

  def csv2parq(csvPath: String, parquetOutputDir: String, withHeader: Boolean = true, overrideMode: Boolean = true) = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
      //Local run mode:org.apache.spark.SparkException: A master URL must be set in your configuration
      .setMaster("local[1]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    log.info("csvPath:" + csvPath)
    val path: File = new File(csvPath);
    log.info("filePath:" + path + "," + path.exists())
    if (!path.exists()) {
      throw new IllegalArgumentException("CSV path not exist:" + csvPath)
    }
    val currentDate = new SimpleDateFormat("yyyyMMdd").format(new Date())
    val subs = subFiles(path)
    log.info("getCsvsFiles:" + subs)
    for (subFile <- subs) {
      val subFilePath = subFile.toString
      val view = "tmp" + currentDate
      log.info("viewName:" + view)
      spark.read.format("csv").option("header", withHeader.toString).load(subFilePath).createOrReplaceTempView(view)
      log.info("subFilePath=" + subFilePath)
      val fileName = subFile.getName.dropRight(CSV_EXT_LEN);
      log.info("fileName:" + fileName)
      val outPath = parquetOutputDir + "/" + fileName + s".parq/run_date=$currentDate"
      log.info("generate data on outputPath: " + outPath)
      val dataSql = spark.sql(s"select * from " + view).repartition(1)
        .write.mode(if (overrideMode) SaveMode.Overwrite else SaveMode.Append).parquet(outPath)
    }
  }

}
