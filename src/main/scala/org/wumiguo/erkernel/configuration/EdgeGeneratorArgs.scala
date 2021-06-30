package org.wumiguo.erkernel.configuration

/**
 * @author levin 
 *         Created on 2021/6/23
 */
case class EdgeGeneratorArgs(
                              executionId: String,
                              appConfigPath: String,
                              kernelConfigPath: String,
                              vertexInputDir: String,
                              edgeOutputDir: String
                            )
