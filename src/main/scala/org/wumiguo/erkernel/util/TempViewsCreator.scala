package org.wumiguo.erkernel.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wumiguo.erkernel.common.Loggable
import org.wumiguo.erkernel.model.DataSourcePath

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object TempViewsCreator extends Loggable {

  def adjustViewName(dataSource: String): String = dataSource.replaceAll("-", "_")

  /**
   * create multiple temp view for all the given data sources
   *
   * @param dataSourcePaths
   * @param spark
   * @return
   */
  def createTempViewPerDataSource(dataSourcePaths: Map[String, DataSourcePath])(implicit spark: SparkSession): Map[String, DataFrame] = {
    for ((dataSource, dataSourcePath) <- dataSourcePaths) yield {
      val view: String = adjustViewName(dataSource)
      LOG.info(s"include data source $dataSource")
      dataSourcePath.partitionPaths.foreach(pp => LOG.info(s"partitionedPath $pp"))
      val df = spark.read.option("basePath", dataSourcePath.basePath)
        .parquet(dataSourcePath.partitionPaths: _*)
      LOG.info(s"DataFrame $view has ${df.rdd.getNumPartitions} partitions")
      val finalDf = df
      LOG.info(s"createOrReplaceTempView view ${view}")
      finalDf.createOrReplaceTempView(view)
      if (LOG.isInfoEnabled()) {
        val count = spark.sql(s"select count(1) from $view").rdd.first()
        LOG.info(s"tempView ${view} has $count records")
      }
      view -> finalDf
    }
  }
}
