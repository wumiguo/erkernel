package org.wumiguo.erkernel.io

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.erkernel.model.{DataSource, DataSourceDict, DataSourcePath}
import org.wumiguo.erkernel.testutil.{SparkTestingEnvSetup, TestDirs}

/**
 * @author levin 
 *         Created on 2021/6/24
 */
class DataSourceLoaderTest extends AnyFlatSpec with SparkTestingEnvSetup {
  it should "extractDataSourceName" in {
    val jsonPath = "/demo/pipelineDataSources.parq"
    val dataSourceName = DataSourceLoader.extractDataSourceName(jsonPath)
    assertResult(dataSourceName)("pipelineDataSources")
  }
  it should "getLastDataSourcePaths" in {
    val dataSourceDataDir = TestDirs.resolveDataPath("")
    var dataSourceDict = DataSourceDict(Map("csdn" -> DataSource(
      "csdn", List("run_date"), "distinct", "1=1",
      Seq(("A", "B"))
    )))
    var dataSourcePath = DataSourceLoader.getLastDataSourcePaths(dataSourceDataDir, dataSourceDict)
    assertResult(dataSourcePath)(Map())
    dataSourceDict = DataSourceDict(Map("glb_booking" -> DataSource(
      "glb_booking", List("run_date"), "distinct", "1=1",
      Seq(("id", "id"))
    )))
    dataSourcePath = DataSourceLoader.getLastDataSourcePaths(dataSourceDataDir, dataSourceDict)
    assertResult(dataSourcePath)(Map(
      "glb_booking" -> DataSourcePath(
        "file:/Users/mac/Downloads/erkernel/target/test-classes/data/glb_booking.parq",
        Seq("file:/Users/mac/Downloads/erkernel/target/test-classes/data/glb_booking.parq/run_date=20210628")
      )
    ))
    log.info(s"dataSourcePath=$dataSourcePath")
  }
}
