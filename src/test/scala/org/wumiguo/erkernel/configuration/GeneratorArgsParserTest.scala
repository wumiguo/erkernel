package org.wumiguo.erkernel.configuration

import org.scalatest.flatspec.AnyFlatSpec

/**
 * @author levin 
 *         Created on 2021/6/24
 */
class GeneratorArgsParserTest extends AnyFlatSpec {
  it should "parse args" in {
    val args = Array("-execId", "001", "-appConfPath", "application.yml", "-kernelConfPath", "erKernel.yml", "-dataSourcesDir", "pipelineDataSources.json")
    val result = GeneratorArgsParser.collectVertexArgs(args)
    assertResult(result.executionId)("001")
    assertResult(result.appConfigPath)("application.yml")
    assertResult(result.kernelConfigPath)("erKernel.yml")
  }
}
