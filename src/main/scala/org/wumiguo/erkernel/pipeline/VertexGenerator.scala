package org.wumiguo.erkernel.pipeline

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wumiguo.erkernel.configuration.GeneratorArgsParser
import org.wumiguo.erkernel.io.{DataSourceConfigLoader, DataSourceLoader}
import org.wumiguo.erkernel.model.{DataSource, DataSourceDict}
import org.wumiguo.erkernel.standardization.FieldStandardiser
import org.wumiguo.erkernel.util.TempViewsCreator
import org.apache.spark.sql.functions._
import org.wumiguo.erkernel.common.ERKernelError

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object VertexGenerator extends Generator {
  /**
   * PUK is a column to determine or provide value to represent a unique entity
   */
  val DEFAULT_PUK_COLUMN = "id_card_no"

  val PRINT_JSON_SAMPLE = true

  def selectFiltering(dataSource: String, dsConfig: DataSource, pukColumn: String = DEFAULT_PUK_COLUMN)(implicit spark: SparkSession): DataFrame = {
    val selectStmt = dsConfig.columnSpec.map {
      case (fieldMapTo, fieldMapFrom) => {
        s"$fieldMapFrom as $fieldMapTo"
      }
    }.mkString(",")
    val distinctMethod = dsConfig.distinctBy
    val distinctStmt = if ("distinct" == distinctMethod) "DISTINCT" else ""
    val dedupeByKeyStmt = if ("key" == distinctMethod) " WHERE dedupe_ordering_key = 1 " else " where 1=1 "
    val filterStmt = if (dsConfig.filterTerm == "") "" else s" WHERE ( ${dsConfig.filterTerm} ) "
    val dsView = dataSource.replaceAll("-", "_")
    val sql =
      s"""
         select $distinctStmt ds.*
         from (
             select row_number() over (partition by $pukColumn order by null ) dedupe_ordering_key, rawDs.*
             from (select $selectStmt
                 from $dsView
                 $filterStmt
              ) as rawDs
         ) ds
         $dedupeByKeyStmt
         """.stripMargin
    LOG.info(s"selectFiltering sql : $sql")
    val df = spark.sql(sql)
    df
  }


  def selectAndEnrich(dataSource: String, config: DataSource, outputDir: String, execId: String, pukColumn: String = DEFAULT_PUK_COLUMN)(implicit spark: SparkSession): DataFrame = {
    val df = selectFiltering(dataSource, config)
    val currentTime = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").format(LocalDateTime.now)
    val enrichedDf = df.transform(FieldStandardiser.mapEmptyToNull)
      .withColumn("party_unique_key", org.apache.spark.sql.functions.xxhash64(col(s"$pukColumn")))
      .withColumn("execution_id", lit(execId))
      .withColumn("execution_timestamp", lit(currentTime))
      .withColumn("source", lit(dataSource))
    //standardization
    val outputDataSourcePath = s"${outputDir}/dataSource=${config.sourceName}"
    LOG.info(s"save ${dataSource} output  to ${outputDataSourcePath}")
    if (PRINT_JSON_SAMPLE) {
      val samplingOutputPath = s"${outputDir}/../sampleVJson/dataSource=${config.sourceName}.json"
      enrichedDf.sample(0.15).write.mode(SaveMode.Overwrite).json(samplingOutputPath)
    }
    enrichedDf.write.mode(SaveMode.Overwrite).parquet(outputDataSourcePath)
    enrichedDf
  }

  override def generate(args: Array[String])(implicit spark: SparkSession): Unit = {
    LOG.info("will execute vertex generation")
    val generatorArgs = GeneratorArgsParser.collectVertexArgs(args)
    LOG.info(s"args: $generatorArgs")
    val jsonPath = generatorArgs.kernelConfigPath
    val dataSourcesDir = generatorArgs.dataSourcesDir
    val dataSourceDict = DataSourceConfigLoader.loadJsonFileAsDataSourceDict(jsonPath)
    val dict = DataSourceLoader.getLastDataSourcePaths(dataSourcesDir, dataSourceDict)
    if (dict.size < dataSourceDict.dict.size) {
      throw ERKernelError(s"Not All dataSources are loaded, please check if dataSourceName and its parquet data folder name are match [$dict vs ${dataSourceDict.dict}]")
    }
    LOG.info("the dataSourcesPath:" + dict)
    TempViewsCreator.createTempViewPerDataSource(dict)
    val outputDir = generatorArgs.vertexOutputDir
    val execId = generatorArgs.executionId
    dataSourceDict.dict.par.foreach {
      case (dataSource, config) => {
        LOG.info(s"reading dataSource $dataSource")
        val df = selectAndEnrich(dataSource, config, outputDir, execId)
      }
    }
  }
}
