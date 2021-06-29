package org.wumiguo.erkernel

import org.wumiguo.erkernel.util.CSV2ParquetFilesUtil.csv2parq

/**
 * @author levin 
 *         Created on 2021/6/26
 */
object DataPrep {

  def main(args: Array[String]): Unit = {
    val csvPath: String = "./data/csvs"
    val parquetOutputDir: String = "./data/parqs"
    val withHeader: Boolean = true
    val overrideMode: Boolean = true
    csv2parq(csvPath, parquetOutputDir, withHeader, overrideMode)
  }

}
