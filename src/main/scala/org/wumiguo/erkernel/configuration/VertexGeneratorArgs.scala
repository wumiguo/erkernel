package org.wumiguo.erkernel.configuration

/**
 * @author levin 
 *         Created on 2021/6/23
 */
case class VertexGeneratorArgs(
                          executionId: String,
                          appConfigPath: String,
                          kernelConfigPath: String,
                          dataSourcesDir: String,
                          vertexOutputDir: String
                        )
