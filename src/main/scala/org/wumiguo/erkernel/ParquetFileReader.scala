package org.wumiguo.erkernel

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object ParquetFileReader {
  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(this.getClass.getName)
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
      //org.apache.spark.SparkException: A master URL must be set in your configuration
      .setMaster("local[1]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val path: File = new File("./data/generatedParquets/csvs/2020");
    val currentDate = new SimpleDateFormat("yyyyMMddhhmm").format(new Date())
    for (dir <- subFiles(path)) {
      val folderName = dir.toString
      val view = "tmp" + currentDate
      log.info("viewName:" + view)
      spark.read.format("csv").option("header", "true").load(folderName).createOrReplaceTempView(view)
      val dates = Array("2020")
      log.info("folderName=" + folderName)
      val fileName = path.getName;
      log.info("fileName:" + fileName)
      for (date <- dates) {
        val outPath = "./data/generatedParquets/" + fileName + s"/$date"
        log.info("generate data on outputPath: " + outPath)
        val dataSql = spark.sql(s"select * from " + view + s" where substr(date,1,4)=$date").repartition(1)
          .write.mode("Append").parquet(outPath)
      }
    }
  }

  def subFiles(dir: File): Iterator[File] = {
    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.toIterator
    f ++ d.toIterator.flatMap(subFiles _)
  }
}
