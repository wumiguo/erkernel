package org.wumiguo.erkernel.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.wumiguo.erkernel.common.Loggable

/**
 * @author levin 
 *         Created on 2021/6/30
 */
object DataSampler extends Loggable {
  private val RUN_JSON_SAMPLE = true
  private val SAMPLE_FRACTION = 0.15
  private val DEFAULT_TYPE = "json"

  def sample(df: DataFrame, filePath: String): Unit = {
    if (RUN_JSON_SAMPLE) {
      val samplePath = if (filePath.endsWith(".json") || filePath.endsWith(".csv")) {
        s"$filePath"
      } else {
        s"${filePath}.${DEFAULT_TYPE}"
      }
      if (samplePath.endsWith(DEFAULT_TYPE)) {
        df.sample(SAMPLE_FRACTION).write.mode(SaveMode.Overwrite).csv(samplePath)
      } else {
        df.sample(SAMPLE_FRACTION).write.mode(SaveMode.Overwrite).json(samplePath)
      }
    }
  }
}

