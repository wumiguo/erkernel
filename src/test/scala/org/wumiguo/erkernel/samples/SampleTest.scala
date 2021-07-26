package org.wumiguo.erkernel.samples

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.erkernel.common.Loggable
import org.wumiguo.erkernel.testutil.SparkTestingEnvSetup
import org.apache.spark.sql.functions._

/**
 * @author levin 
 *         Created on 2021/7/3
 */
class SampleTest extends AnyFlatSpec with SparkTestingEnvSetup with Loggable {
  it should "xxhash64" in {
    def hash(implicit cardNo: String) = {
      val df = spark.sql(s"select '${cardNo}' as _key")
      val df2 = df.withColumn("_hash64key", xxhash64(df.col("_key")))
        .drop("_key")
      df2.rdd.map(_.getLong(0)).collect.toSeq(0)
    }

    implicit var cardNo = "441550208901014231"
    assertResult(-8594282087851469328L)(hash)
    cardNo = "441550210901014231"
    assertResult(-3364822206197342846L)(hash)
    cardNo = "SomeTextInput123 Test"
    assertResult(1193846228511159618L)(hash)

  }
}
