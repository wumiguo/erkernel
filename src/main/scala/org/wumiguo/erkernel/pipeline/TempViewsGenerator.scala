package org.wumiguo.erkernel.pipeline

import org.apache.spark.sql.SparkSession

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object TempViewsGenerator extends Generator {
  override def generate(args: Array[String])(implicit spark: SparkSession): Unit = {

  }
}
