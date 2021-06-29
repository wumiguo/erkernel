package org.wumiguo.erkernel.io

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.wumiguo.erkernel.model.{DataSource, DataSourceDict, DataSourcePath}
import org.wumiguo.erkernel.util.JsonReader

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object DataSourceConfigLoader {
  def loadJsonFileAsDataSourceDict(jsonPath: String): DataSourceDict = {
    val data = JsonReader.readJson2Map(jsonPath)
    DataSourceDict(data.keySet.map(
      dataSourceKey => {
        val dataSourceConfig = data(dataSourceKey).asInstanceOf[Map[String, Any]]
        dataSourceKey -> DataSource(
          sourceName = if (!dataSourceConfig.contains("name")) {
            dataSourceKey
          } else {
            dataSourceConfig("name").asInstanceOf[String]
          },
          partitions = dataSourceConfig("partitions").asInstanceOf[List[String]],
          distinctBy = dataSourceConfig("distinctBy").asInstanceOf[String].toUpperCase,
          filterTerm = dataSourceConfig("filterConditions").asInstanceOf[String],
          columnSpec = dataSourceConfig("columnsPicker").asInstanceOf[Map[String, String]].toSeq.sorted
        )
      }
    ).toMap)
  }
}
