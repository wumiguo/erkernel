package org.wumiguo.erkernel.io

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.erkernel.model.DataSource
import org.wumiguo.erkernel.testutil.TestDirs

/**
 * @author levin 
 *         Created on 2021/6/24
 */
class DataSourceConfigLoaderTest extends AnyFlatSpec {
  it should "loadJsonFileAsDataSourceDict" in {
    val jsonPath = TestDirs.resolvePath("pipelineDataSources.json")
    val result = DataSourceConfigLoader.loadJsonFileAsDataSourceDict(jsonPath)
    assertResult(result.dict("csdn"))(DataSource("CSDN", List(), "", "1=1",
      Seq(("author", "blogger"), ("city", "city"), ("education", "education"),
        ("id_card_no", "id_card_no"), ("location", "concat_ws(',',province,city)"),
        ("province", "province"), ("published_date", "published_date"), ("title", "job_title")))
    )
  }
}
