package org.wumiguo.erkernel.util

import scala.util.parsing.json

/**
 * @author levin 
 *         Created on 2021/6/22
 */
object JsonReader {


  def readJson(jsonPath: String): Any = {
    val jsonIn = scala.io.Source.fromFile(jsonPath, "utf-8")
    val jsonStr = try jsonIn.getLines.mkString finally jsonIn.close
    parseJson(jsonStr)
  }

  def parseJson(jsonStr: String): Any = {
    json.JSON.parseFull(jsonStr) match {
      case Some(data) => data
      case None => throw new IllegalArgumentException(s"Invalid json string : $jsonStr")
    }
  }


  def json2Map(jsonStr: String): Map[String, Map[String, Any]] = {
    val data = parseJson(jsonStr)
    data.asInstanceOf[Map[String, Map[String, Any]]]
  }

  def readJson2Map(jsonPath: String): Map[String, Map[String, Any]] = {
    val data = readJson(jsonPath)
    try {
      data.asInstanceOf[Map[String, Map[String, Any]]]
    } catch {
      case e: Exception => throw new IllegalArgumentException("Fail to case map:" + data, e)
    }
  }
}
