package org.wumiguo.erkernel.pipeline

import org.apache.spark.sql.SparkSession

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object MiniGenerator extends Generator {
  override def generate(args: Array[String])(implicit spark: SparkSession): Unit = {
    //load different data sets

    // load edge rules

    // generate graph
  }
}
