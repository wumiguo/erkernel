package org.wumiguo.erkernel.io

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.erkernel.model.{GraphResultSetting, RelationShip}
import org.wumiguo.erkernel.testutil.TestDirs

/**
 * @author levin 
 *         Created on 2021/6/25
 */
class GraphResultSettingConfigLoaderTest extends AnyFlatSpec {
  it should "loadJsonFileAsGraphResultSettingDict" in {
    val jsonPath = TestDirs.resolvePath("pipelineGraphRecord.json")
    val result = GraphResultSettingConfigLoader.loadJsonFileAsGraphResultSettingDict(jsonPath)
    assertResult(result.dict("connected_party_with_the_inherent"))(
      GraphResultSetting("connected_party_with_the_inherent", "connectedPartyTriplets.parq", "connectedPartyVertices.parq",
        "ccaid", List("the_inherent"), List())
    )
  }
}
