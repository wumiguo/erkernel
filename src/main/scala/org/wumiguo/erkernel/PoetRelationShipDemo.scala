package org.wumiguo.erkernel

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * Demo: 诗人之间的关系
 *
 * @author levin 
 *         Created on 2021/6/23
 */
object PoetRelationShipDemo {

  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(this.getClass.getName)
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
      //org.apache.spark.SparkException: A master URL must be set in your configuration
      .setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val userList: List[(Long, String)] = List(
      (1L, "李白"), (2L, "杜甫"), (3L, "王安石"), (4L, "王维"),
      (5L, "白居易"), (6L, "辛弃疾"), (7L, "李贺"), (8L, "李清照")
    )
    log.info("users:" + userList)
    val vertices: RDD[(Long, String)] = sc.makeRDD(userList)
    val relationShip1: List[Edge[Int]] = List(
      Edge(1, 2, 0), Edge(1, 3, 0), Edge(1, 4, 0), Edge(1, 5, 0), Edge(1, 6, 0),
      Edge(2, 3, 0), Edge(2, 5, 0),
      Edge(3, 4, 0),
      Edge(7, 6, 2), Edge(7, 8, 0)
    )
    log.info("relationShip1:" + relationShip1)
    val edges1: RDD[Edge[Int]] = sc.makeRDD(relationShip1)
    //generate relationship graph
    val g1: Graph[String, Int] = Graph(vertices, edges1)
    val ccVertices1 = g1.connectedComponents().vertices
    log.info("complete graph generation")
    ccVertices1.sortBy(_._1).foreach(x => log.info("vertices1:" + x))
    ccVertices1.join(vertices).map(x => (x._2._1, x._2._2)).reduceByKey(
      (a, b) => a + " 认识 " + b
    ).foreach(x => log.info("relations1:" + x))


    val relationShip2: List[Edge[Int]] = List(
      Edge(1, 2, 0), Edge(1, 3, 0),
      Edge(2, 3, 0), Edge(2, 5, 0),
      Edge(3, 4, 0),
      Edge(7, 6, 2), Edge(7, 8, 0)
    )
    log.info("relationShip2:" + relationShip2)
    val edges2: RDD[Edge[Int]] = sc.makeRDD(relationShip2)
    val g2: Graph[String, Int] = Graph(vertices, edges2)
    val ccVertices2 = g2.connectedComponents().vertices
    log.info("complete graph generation")
    ccVertices2.sortBy(_._1).foreach(x => log.info("vertices2:" + x))
    ccVertices2.join(vertices).map(x => (x._2._1, x._2._2)).reduceByKey(
      (a, b) => a + " 认识 " + b
    ).foreach(x => log.info("relations2:" + x))


    val relationShip3: List[Edge[Int]] = List(
      Edge(1, 2, 0), Edge(1, 3, 0),
      Edge(7, 6, 2)
    )
    log.info("relationShip3:" + relationShip3)
    val edges3: RDD[Edge[Int]] = sc.makeRDD(relationShip3)
    val g3: Graph[String, Int] = Graph(vertices, edges3)
    val ccVertices3 = g3.connectedComponents().vertices
    log.info("complete graph generation")
    ccVertices3.sortBy(_._1).foreach(x => log.info("vertices3:" + x))
    ccVertices3.join(vertices).map(x => (x._2._1, x._2._2)).reduceByKey(
      (a, b) => a + " 认识 " + b
    ).foreach(x => log.info("relations3:" + x))

  }
}
