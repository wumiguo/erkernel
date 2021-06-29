package org.wumiguo.erkernel.standardization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

/**
 * @author levin 
 *         Created on 2021/6/26
 */
object FieldStandardiser {
  def mapEmptyToNull(df: DataFrame): DataFrame = {
    val colStmt = df.schema.map {
      field =>
        field.dataType match {
          case StringType =>
            when(length(trim(col(field.name))) === 0, lit(null: String).cast(StringType))
              .otherwise(trim(col(field.name))).as(field.name)
          case _ => col(field.name)
        }
    }
    df.select(colStmt: _*)
  }
}
