package org.wumiguo.erkernel.samples

import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.common.{Loggable, SparkEnvSetup}
import org.wumiguo.erkernel.io.RelationShipConfigLoader
import org.wumiguo.erkernel.pipeline.{EdgeGenerator, GraphGenerator, VertexGenerator}

/**
 * @author levin 
 *         Created on 2021/7/01
 */
object E2EGeneratorGCPRunner extends SparkEnvSetup with Loggable {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().appName(getClass.getSimpleName).getOrCreate()
    try {
      LOG.info("runner start")
      val jobDir = "gs://demodata/sparkjob/"
      val vertexJson = s"${jobDir}/vertexDataSources.json"
      var dataSourcesDir = "gs://demodata/sparkjob/data/parqs"
      val executionId = "EXEC20210701"
      var vertexOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/vertex"
      LOG.info("vertexJson:" + vertexJson)
      var args = Array("-execId", executionId, "-appConfPath", "application.yml",
        "-kernelConfPath", vertexJson, "-dataSourcesDir", dataSourcesDir,
        "-vertexOutputDir", vertexOutputDir)
      VertexGenerator.generate(args)
      val edgeJson = s"${jobDir}/relationShipSpec.json"
      //val edgeJson = getClass.getClassLoader.getResource("./relationShipSpec.json").getPath
      val result = RelationShipConfigLoader.loadJsonFileAsRelationShipDict(edgeJson)
      dataSourcesDir = "/Users/mac/Downloads/erkernel/data/parqs"
      vertexOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/vertex"
      val edgeOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/edge"
      LOG.info("edgeJson:" + edgeJson)
      args = Array("-execId", executionId, "-appConfPath", "application.yml",
        "-edgeConfPath", edgeJson, "-vertexOutputDir", vertexOutputDir,
        "-edgeOutputDir", edgeOutputDir)
      EdgeGenerator.generate(args)
      //change to externalized it
      //val graphJson = getClass.getClassLoader.getResource("./graphRecord.json").getPath
      val graphJson = s"${jobDir}/graphRecord.json"
      val vertexInputDir = vertexOutputDir
      val edgeInputDir =edgeOutputDir
      val graphOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/graph"
      LOG.info("graphJson:" + graphJson)
      args = Array("-execId", executionId, "-appConfPath", "application.yml",
        "-vertexInputDir", vertexInputDir,
        "-edgeConfPath", graphJson, "-edgeInputDir", edgeInputDir,
        "-graphOutputDir", graphOutputDir)
      GraphGenerator.generate(args)
    } catch {
      case e: Exception => {
        LOG.error("detect error on runner", e)
      }
    } finally {
      LOG.info("runner end")
    }
  }
}
