package org.wumiguo.erkernel.pipeline

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wumiguo.erkernel.configuration.GeneratorArgsParser
import org.wumiguo.erkernel.io.{DataSourceConfigLoader, RelationShipConfigLoader}
import org.wumiguo.erkernel.pipeline.VertexGenerator.{LOG, PRINT_JSON_SAMPLE}
import org.apache.spark.sql.functions._
import org.wumiguo.erkernel.model.RelationShip

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object EdgeGenerator extends Generator {
  val PRINT_JSON_SAMPLE = true

  def createEdgeDfPerRelationShip(relationShip: RelationShip)(implicit spark: SparkSession): DataFrame = {
    LOG.info(s"Apply relationship ${relationShip.relationId}")
    val nonSelfJoin = " and subject.party_unique_key <> counterParty.party_unique_key "
    val saltCondition = " and subject.salt_id = counterParty.salt_id "
    val sql =
      s"""
         with subject as (select * from dataSourceVertices where source = '${relationShip.subject}' ),
         with counterParty as (select * from dataSourceVertices where source = '${relationShip.counterParty}' )
         select distinct
            least(subject.party_unique_key, counterParty.party_unique_key ) party_unique_key_from,
            '${relationShip.relationShipLabel}' as edge_property,
            greatest(subject.party_unique_key, counterParty.party_unique_key ) party_unique_key_to,
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
    LOG.info("will execute vertex generation")
    val generatorArgs = GeneratorArgsParser.collectEdgeArgs(args)
    LOG.info(s"args: $generatorArgs")
    val jsonPath = generatorArgs.kernelConfigPath
    val dataSourcesDir = generatorArgs.dataSourcesDir
    val edgeOutputDir = generatorArgs.vertexOutputDir
    val relationShipDict = RelationShipConfigLoader.loadJsonFileAsRelationShipDict(jsonPath)
    val dataSourceVertices = spark.read.option("mergeSchema", "true").parquet(dataSourcesDir)
    dataSourceVertices.createOrReplaceTempView("dataSourceVertices")
    val saltFactor = 16
    val vertexDf = spark.sql(s"select dv.*, abs(mod(party_unique_key,$saltFactor)) salt_id from dataSourceVertices dv")
    vertexDf.createOrReplaceTempView("vertexDf")
    val vertexDfExpanded = spark.sql(s"select dv.* from dataSourceVertices dv")
      .withColumn("salt_array", array(Range(0, saltFactor).toList.map(lit): _*))
    val currentTime = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").format(LocalDateTime.now)
    val execId = generatorArgs.executionId
    relationShipDict.dict.par.foreach {
      case (_, relationShip) => {
        val df = createEdgeDfPerRelationShip(relationShip)
          .withColumn("relationship", lit(relationShip.relationId))
          .withColumn("execution_id", lit(execId))
          .withColumn("execution_timestamp", lit(currentTime))
        val outputPath = s"$edgeOutputDir/relationship=${relationShip.relationId}"
        df.write.mode(SaveMode.Overwrite).parquet(outputPath)
        if (PRINT_JSON_SAMPLE) {
          df.sample(0.15).write.mode(SaveMode.Overwrite).json(outputPath + ".json")
        }
      }
    }
  }
}
