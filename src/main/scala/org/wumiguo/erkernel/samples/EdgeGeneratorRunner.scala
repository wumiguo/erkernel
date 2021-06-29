package org.wumiguo.erkernel.samples

import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.common.{Loggable, SparkEnvSetup}
import org.wumiguo.erkernel.io.RelationShipConfigLoader
import org.wumiguo.erkernel.pipeline.{EdgeGenerator, VertexGenerator}
import org.wumiguo.erkernel.samples.VertexGeneratorRunner.{LOG, getClass}

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object EdgeGeneratorRunner extends SparkEnvSetup with Loggable {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession(this.getClass.getName)
    try {
      LOG.info("runner start")
      val edgeJson = getClass.getClassLoader.getResource("./pipelineRelationShipSpec.json").getPath
      val result = RelationShipConfigLoader.loadJsonFileAsRelationShipDict(edgeJson)
      val dataSourcesDir = "/Users/mac/Downloads/erkernel/data/parqs"
      val executionId = "EXEC20210627"
      val vertexOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/vertex"
      val edgeOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/vertex"
      LOG.info("edgeJson:" + edgeJson)
      val args = Array("-execId", executionId, "-appConfPath", "application.yml",
        "-edgeConfPath", edgeJson, "-dataSourcesDir", vertexOutputDir,
        "-edgeOutputDir", edgeOutputDir)
      EdgeGenerator.generate(args)
    } catch {
      case e: Exception => {
        LOG.error("detect error on runner", e)
      }
    } finally {
      LOG.info("runner end")
    }
  }
}
