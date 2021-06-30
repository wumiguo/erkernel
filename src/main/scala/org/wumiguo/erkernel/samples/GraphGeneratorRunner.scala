package org.wumiguo.erkernel.samples

import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.common.{Loggable, SparkEnvSetup}
import org.wumiguo.erkernel.io.{GraphResultSettingConfigLoader, RelationShipConfigLoader}
import org.wumiguo.erkernel.pipeline.{EdgeGenerator, GraphGenerator}
import org.wumiguo.erkernel.samples.EdgeGeneratorRunner.{LOG, getClass}
import org.wumiguo.erkernel.samples.VertexGeneratorRunner.createLocalSparkSession

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object GraphGeneratorRunner extends SparkEnvSetup with Loggable {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createLocalSparkSession(this.getClass.getName)
    try {
      LOG.info("runner start")
      val graphJson = getClass.getClassLoader.getResource("./graphRecord.json").getPath
      val executionId = "EXEC20210627"
      val vertexInputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/vertex"
      val edgeInputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/edge"
      val graphOutputDir = s"/Users/mac/Downloads/erkernel/data/output/${executionId}/graph"
      LOG.info("graphJson:" + graphJson)
      val args = Array("-execId", executionId, "-appConfPath", "application.yml",
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
