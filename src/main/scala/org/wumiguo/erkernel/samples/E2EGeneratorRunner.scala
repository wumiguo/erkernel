package org.wumiguo.erkernel.samples

import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.common.{Loggable, SparkEnvSetup}

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object E2EGeneratorRunner extends SparkEnvSetup with Loggable {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession(this.getClass.getName)
    try {
      LOG.info("runner start")

    } catch {
      case e: Exception => {
        LOG.error("detect error on runner", e)
      }
    } finally {
      LOG.info("runner end")
    }
  }
}
