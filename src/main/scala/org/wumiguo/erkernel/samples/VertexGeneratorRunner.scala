package org.wumiguo.erkernel.samples

import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.common.{Loggable, SparkEnvSetup}
import org.wumiguo.erkernel.pipeline.VertexGenerator

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object VertexGeneratorRunner extends SparkEnvSetup with Loggable {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createLocalSparkSession(this.getClass.getName)
    try {
      LOG.info("runner start")
      val vertexJson = getClass.getClassLoader.getResource("./vertexDataSources.json").getPath
      val dataSourcesDir = "/Users/mac/Downloads/erkernel/data/parqs"
      val executionId = "EXEC20210627"
      val vertexOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/vertex"
      LOG.info("vertexJson:" + vertexJson)
      val args = Array("-execId", executionId, "-appConfPath", "application.yml",
        "-kernelConfPath", vertexJson, "-dataSourcesDir", dataSourcesDir,
        "-vertexOutputDir", vertexOutputDir)
      VertexGenerator.generate(args)
    } catch {
      case e: Exception => {
        LOG.error("detect error on runner", e)
      }
    } finally {
      LOG.info("runner end")
    }
  }
}
