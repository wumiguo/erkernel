package org.wumiguo.erkernel.pipeline

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wumiguo.erkernel.configuration.GeneratorArgsParser
import org.wumiguo.erkernel.io.{DataSourceConfigLoader, RelationShipConfigLoader}
import org.apache.spark.sql.functions._
import org.wumiguo.erkernel.model.RelationShip
import org.wumiguo.erkernel.util.DataSampler

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object EdgeGenerator extends Generator {

  def createEdgeDfPerRelationShip(relationShip: RelationShip)(implicit spark: SparkSession): DataFrame = {
    LOG.info(s"Apply relationship ${relationShip.relationId}")
    val nonSelfJoin = " and subject.party_unique_key <> counterParty.party_unique_key "
    val saltCondition = " and subject.salt_id = counterParty.salt_id "
    val sql =
      s"""
         with
            subject as (select * from vertexDfSalted where source = '${relationShip.subject}' ),
            counterParty as (select * from vertexDfExpanded where source = '${relationShip.counterParty}' )
         select distinct
            least(subject.party_unique_key, counterParty.party_unique_key ) party_unique_key_from,
            '${relationShip.relationShipLabel}' as edge_property,
            greatest(subject.party_unique_key, counterParty.party_unique_key ) party_unique_key_to
         from subject
         inner join counterParty
         on (
            (${relationShip.relationLogic})
              $saltCondition
              $nonSelfJoin
         )
         where 1=1
         """.stripMargin
    LOG.info(s"relationShipEdgeSql: $sql")
    val df = spark.sql(sql)
    df
  }

  override def generate(args: Array[String])(implicit spark: SparkSession): Unit = {
    LOG.info("will execute edge generation")
    val generatorArgs = GeneratorArgsParser.collectEdgeArgs(args)
    LOG.info(s"args: $generatorArgs")
    val jsonPath = generatorArgs.kernelConfigPath
    val vertexInputDir = generatorArgs.vertexInputDir
    val execId = generatorArgs.executionId
    val edgeOutputDir = generatorArgs.edgeOutputDir
    val relationShipDict = RelationShipConfigLoader.loadJsonFileAsRelationShipDict(jsonPath)
    val dataSourceVertices = spark.read.option("mergeSchema", "true").parquet(vertexInputDir)
    DataSampler.sample(dataSourceVertices, s"${edgeOutputDir}/../samples/e01VMerged/execution=${execId}")

    dataSourceVertices.createOrReplaceTempView("dataSourceVertices")
    val saltFactor = 16
    val vertexDfSalted = spark.sql(s"select dv.*, abs(mod(party_unique_key,$saltFactor)) salt_id from dataSourceVertices dv")
    vertexDfSalted.createOrReplaceTempView("vertexDfSalted")
    val vertexDfExpanded = spark.sql(s"select dv.* from dataSourceVertices dv")
      .withColumn("salt_array", array(Range(0, saltFactor).toList.map(lit): _*))
    val vertexDfSaltedExpanded = vertexDfExpanded.withColumn("salt_id", explode(vertexDfExpanded("salt_array")))
      .drop("salt_array")
    vertexDfSaltedExpanded.createOrReplaceTempView("vertexDfExpanded")

    val currentTime = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").format(LocalDateTime.now)
    relationShipDict.dict.par.foreach {
      case (_, relationShip) => {
        val df = createEdgeDfPerRelationShip(relationShip)
          .withColumn("relationship", lit(relationShip.relationId))
          .withColumn("execution_id", lit(execId))
          .withColumn("execution_timestamp", lit(currentTime))
        val outputPath = s"$edgeOutputDir/relationship=${relationShip.relationId}"
        df.write.mode(SaveMode.Overwrite).parquet(outputPath)
        DataSampler.sample(df, s"${edgeOutputDir}/../samples/e02ESamples/relationship=${relationShip.relationId}_execution=${execId}")
      }
    }
  }
}
