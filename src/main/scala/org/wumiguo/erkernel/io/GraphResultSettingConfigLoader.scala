package org.wumiguo.erkernel.io

import org.slf4j.LoggerFactory
import org.wumiguo.erkernel.model.{GraphResultSetting, GraphResultSettingDict, RelationShip, RelationShipDict}
import org.wumiguo.erkernel.util.JsonReader

/**
 * @author levin 
 *         Created on 2021/6/25
 */
object GraphResultSettingConfigLoader {
  def loadJsonFileAsGraphResultSettingDict(jsonPath: String): GraphResultSettingDict = {
    val data = JsonReader.readJson2Map(jsonPath)
    GraphResultSettingDict(data.keySet.map(
      graphKey => {
        val dataSourceConfig = data(graphKey).asInstanceOf[Map[String, Any]]
        //LoggerFactory.getLogger(getClass.getName).info("dataSourceConfig:" + dataSourceConfig)
        graphKey -> GraphResultSetting(
          graphId = if (!dataSourceConfig.contains("graphId")) {
            graphKey
          } else {
            dataSourceConfig("graphId").asInstanceOf[String]
          },
          tripletsOutputName = dataSourceConfig("triplets_output_name").asInstanceOf[String],
          vertexOutputName = dataSourceConfig("vertex_output_name").asInstanceOf[String],
          connectedComponentAttrName = dataSourceConfig("connected_comp_attr_name").asInstanceOf[String],
          relationshipInclusion = dataSourceConfig("relationship_inclusion").asInstanceOf[List[String]],
          relationshipExclusion = dataSourceConfig("relationship_exclusion").asInstanceOf[List[String]]
        )
      }
    ).toMap)
  }
}
