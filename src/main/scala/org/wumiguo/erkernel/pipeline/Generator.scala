package org.wumiguo.erkernel.pipeline

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.wumiguo.erkernel.common.{Loggable, SparkEnvSetup}
import org.wumiguo.erkernel.pipeline.EdgeGenerator.RUN_JSON_SAMPLE

/**
 * A generator is a program to generate the shape data like vertices, edges, graphs
 *
 * @author levin 
 *         Created on 2021/6/21
 */
trait Generator extends Serializable with SparkEnvSetup with Loggable {
  val RUN_JSON_SAMPLE = true

  def generate(args: Array[String])(implicit spark: SparkSession): Unit
}
