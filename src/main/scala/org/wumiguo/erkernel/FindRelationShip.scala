package org.wumiguo.erkernel

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author levin 
 *         Created on 2021/6/23
 */
object FindRelationShip {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

  }
}
