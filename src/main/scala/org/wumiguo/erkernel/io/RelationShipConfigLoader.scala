package org.wumiguo.erkernel.io

import org.wumiguo.erkernel.model.{DataSource, DataSourceDict, RelationShip, RelationShipDict}
import org.wumiguo.erkernel.util.JsonReader

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object RelationShipConfigLoader {
  def loadJsonFileAsRelationShipDict(jsonPath: String): RelationShipDict = {
    val data = JsonReader.readJson2Map(jsonPath)
    RelationShipDict(data.keySet.map(
      relationKey => {
        val dataSourceConfig = data(relationKey).asInstanceOf[Map[String, Any]]
        relationKey -> RelationShip(
          relationId = if (!dataSourceConfig.contains("relationId")) {
            relationKey
          } else {
            dataSourceConfig("relationId").asInstanceOf[String]
          },
          subject = dataSourceConfig("subject").asInstanceOf[String],
          counterParty = dataSourceConfig("counterParty").asInstanceOf[String],
          relationShipLabel = dataSourceConfig("relationShipLabel").asInstanceOf[String],
          relationLogic = dataSourceConfig("relationLogic").asInstanceOf[String]
        )
      }
    ).toMap)
  }
}
