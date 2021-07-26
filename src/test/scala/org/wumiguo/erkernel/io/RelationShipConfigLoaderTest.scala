package org.wumiguo.erkernel.io

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.erkernel.model.{RelationShip}
import org.wumiguo.erkernel.testutil.TestDirs

/**
 * @author levin 
 *         Created on 2021/6/25
 */
class RelationShipConfigLoaderTest extends AnyFlatSpec {
  it should "loadJsonFileAsRelationShipDict" in {
    val jsonPath = TestDirs.resolvePath("pipelineRelationShipSpec.json")
    val result = RelationShipConfigLoader.loadJsonFileAsRelationShipDict(jsonPath)
    assertResult(result.dict("inherent_alumni_baiduxueshu"))(
      RelationShip("inherent_alumni_baiduxueshu", "baiduxueshu", "baiduxueshu", "the_inherent", "subject.school=counterParty.school")
    )
  }
}
