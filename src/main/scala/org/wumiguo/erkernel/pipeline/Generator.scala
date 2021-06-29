package org.wumiguo.erkernel.pipeline

import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.common.{Loggable, SparkEnvSetup}

/**
 * A generator is a program to generate the shape data like vertices, edges, graphs
 *
 * @author levin 
 *         Created on 2021/6/21
 */
trait Generator extends Serializable with SparkEnvSetup with Loggable {
  def generate(args: Array[String])(implicit spark: SparkSession): Unit
}
