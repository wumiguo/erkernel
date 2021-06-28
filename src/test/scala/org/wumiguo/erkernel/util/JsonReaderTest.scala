package org.wumiguo.erkernel.util

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.erkernel.testutil.TestDirs

/**
 * @author levin 
 *         Created on 2021/6/24
 */
class JsonReaderTest extends AnyFlatSpec {
  it should "parse json" in {
    var jsonStr = "{\"name\":\"levin\"}"
    var result = JsonReader.parseJson(jsonStr)
    assertResult(result)(Map("name" -> "levin"))
    jsonStr = "[\"levin\",\"liu\"]"
    result = JsonReader.parseJson(jsonStr)
    assertResult(result)(Array("levin", "liu"))
  }

  it should "json2Map" in {
    var jsonStr = "{\"name\":\"levin\"}"
    var result = JsonReader.json2Map(jsonStr)
    assertResult(result)(Map("name" -> "levin"))
  }

  it should "readJson2Map" in {
    val jsonPath = TestDirs.resolvePath("pipelineDataSources.json")
    val result = JsonReader.readJson2Map(jsonPath)
    assertResult(
      Map("csdn" -> Map("distinctBy" -> "", "name" -> "CSDN", "filterConditions" -> "1=1",
        "columnsPicker" -> Map("author" -> "blogger", "city" -> "city",
          "location" -> "concat_ws(',',province,city)", "id_card_no" -> "id_card_no",
          "education" -> "education", "published_date" -> "published_date",
          "province" -> "province", "title" -> "job_title"),
        "partitions" -> List()),
        "juejin" -> Map("distinctBy" -> "", "name" -> "juejin", "filterConditions" -> "1=1",
          "columnsPicker" -> Map("author" -> "writer", "location" -> "location",
            "id_card_no" -> "idcardno", "education" -> "education", "published_date" -> "published_date",
            "title" -> "occupation"),
          "partitions" -> List()))
    )(result)
  }
}
