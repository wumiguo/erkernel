package org.wumiguo.erkernel.pipeline

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.wumiguo.erkernel.configuration.{GeneratorArgsParser, GraphGeneratorArgs}
import org.wumiguo.erkernel.io.GraphResultSettingConfigLoader
import org.wumiguo.erkernel.model.{GraphResultSetting, TripleModel}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.wumiguo.erkernel.util.DataSampler

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object GraphGenerator extends Generator {

  def connectAndGenerate(config: GraphResultSetting, generatorArgs: GraphGeneratorArgs)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val vertexDf = spark.sql(s"select * from  dataSourceVertices where dedupe_ordering_key = 1 ")
    val selectiveVertexDf = vertexDf.select("party_unique_key", "id_card_no", "author", "group")
      .distinct.rdd
    val svRdd = selectiveVertexDf.map(row => (row.getLong(0), Map[String, String](
      "key" -> row.getString(1),
      "name" -> row.getString(2),
      "group" -> row.getString(3)
    )))
    val execId = generatorArgs.executionId
    DataSampler.sample(svRdd.toDF(), s"${generatorArgs.graphOutputPath}/../samples/g01sampleSelectedVJson/execution=${execId}")

    val relationshipSql =
      s""" select * from relationshipEdges
           where edge_property in (${config.relationshipInclusion.map(value => "'" + value + "'").mkString(",")}) """.stripMargin
    LOG.info(s"relationshipSql: $relationshipSql")
    val relationShipDf = spark.sql(relationshipSql)
    val selectiveRelationshipRdd = relationShipDf.select("party_unique_key_from", "party_unique_key_to", "edge_property", "relationship")
      .distinct.rdd
    val selectiveRelationShipEdgeRdd: RDD[Edge[Array[String]]] = selectiveRelationshipRdd.map(row => Edge(
      row.getLong(0),
      row.getLong(1),
      Array(row.getString(2), row.getString(3)))
    ).cache()
    DataSampler.sample(selectiveRelationShipEdgeRdd.toDF(), s"${generatorArgs.graphOutputPath}/../samples/g02sampleCachedREdgeJson/execution=${execId}")

    val graph = Graph(svRdd, selectiveRelationShipEdgeRdd, Map[String, String](
      "key" -> "NotFound",
      "name" -> "NotFound",
      "group" -> "NotFound"
    ))
    val connectedComponents = graph.connectedComponents().vertices.toDF("vertex_id", "ccId")
      .withColumn(s"${config.connectedComponentAttrName}", expr(
        """case
             when ccId >=0
             then concat('P', cast(abs(ccId) as string))
             else concat('N', cast(abs(ccId) as string))
           end
          """.stripMargin))
      .drop("ccId")
    DataSampler.sample(connectedComponents, s"${generatorArgs.graphOutputPath}/../samples/g03sampleCCVerticesJson/execution=${execId}")
    val triangleDf = graph.triangleCount().vertices.toDF("vertex_id", "triangle_count")
    DataSampler.sample(triangleDf, s"${generatorArgs.graphOutputPath}/../samples/g04sampleTriangleDf/execution=${execId}")

    val algoJoinDF = connectedComponents.join(triangleDf, Seq("vertex_id"), "inner").persist(StorageLevel.MEMORY_AND_DISK)
    DataSampler.sample(algoJoinDF, s"${generatorArgs.graphOutputPath}/../samples/g05sampleAlgoJoinDF/execution=${execId}")

    val vertexEnrichDf = vertexDf
      .join(algoJoinDF, vertexDf("party_unique_key") === algoJoinDF("vertex_id"), "inner")
      .drop("vertex_id")
    DataSampler.sample(vertexEnrichDf, s"${generatorArgs.graphOutputPath}/../samples/g06sampleVertexEnrichDf/execution=${execId}")
    val vertexOutputPath = s"${generatorArgs.graphOutputPath}/${config.vertexOutputName}"
    vertexEnrichDf.write.mode(SaveMode.Overwrite).format("parquet").save(vertexOutputPath)
    val tripletsDf = graph.triplets.map((x: EdgeTriplet[Map[String, String], Array[String]]) => {
      TripleModel(
        idFrom = x.srcId,
        keyFrom = x.srcAttr("key"),
        nameFrom = x.srcAttr("name"),
        groupFrom = x.srcAttr("group"),
        idTo = x.dstId,
        keyTo = x.dstAttr("key"),
        nameTo = x.dstAttr("name"),
        groupTo = x.dstAttr("group"),
        edgeProperty = x.attr(0),
        relationShip = x.attr(1)
      )
    }).toDF()
    DataSampler.sample(tripletsDf, s"${generatorArgs.graphOutputPath}/../samples/g07sampleTripletsDf/execution=${execId}")

    val currentTime = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").format(LocalDateTime.now)
    val tripletsWithCCDF = tripletsDf
      .join(algoJoinDF, algoJoinDF("vertex_id") === tripletsDf("idFrom"), "inner")
      .drop("vertex_id")
      .withColumn("execution_id", lit(execId))
      .withColumn("execution_timestamp", lit(currentTime))
    val tripleOutPath = s"${generatorArgs.graphOutputPath}/${config.tripletsOutputName}"
    tripletsWithCCDF.repartition(50).write.mode(SaveMode.Overwrite).format("parquet").save(tripleOutPath)
    DataSampler.sample(tripletsWithCCDF, s"${generatorArgs.graphOutputPath}/../samples/g08sampleTripletsCCDf/execution=${execId}")
  }


  override def generate(args: Array[String])(implicit spark: SparkSession): Unit = {
    LOG.info("will execute graph generation")
    val generatorArgs = GeneratorArgsParser.collectGraphArgs(args)
    LOG.info(s"args: $generatorArgs")
    val jsonPath = generatorArgs.kernelConfigPath
    val vertexInputDir = generatorArgs.vertexInputDir
    val edgeInputDir = generatorArgs.edgeInputDir
    val graphResultSettingDict = GraphResultSettingConfigLoader.loadJsonFileAsGraphResultSettingDict(jsonPath)
    val dataSourceVertices = spark.read.option("mergeSchema", "true").parquet(vertexInputDir)
    dataSourceVertices.createOrReplaceTempView("dataSourceVertices")
    val relationshipEdges = spark.read.option("mergeSchema", "true").parquet(edgeInputDir)
    relationshipEdges.createOrReplaceTempView("relationshipEdges")
    graphResultSettingDict.dict.par.foreach {
      case (_, config) => {
        connectAndGenerate(config, generatorArgs)
      }
    }
  }
}
