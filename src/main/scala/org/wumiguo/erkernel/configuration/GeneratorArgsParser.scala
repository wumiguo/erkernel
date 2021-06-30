package org.wumiguo.erkernel.configuration

import org.wumiguo.erkernel.common.ERKernelError

/**
 * @author levin 
 *         Created on 2021/6/24
 */
object GeneratorArgsParser {
  final val EXEC_ID = "-execId"
  final val APP_CONF_PATH = "-appConfPath"
  final val KERNEL_CONF_PATH = "-kernelConfPath"
  final val DATA_SOURCE_DIR = "-dataSourcesDir"
  final val VERTEX_OUTPUT_DIR = "-vertexOutputDir"
  final val EDGE_CONF_PATH = "-edgeConfPath"
  final val EDGE_OUTPUT_DIR = "-edgeOutputDir"
  final val VERTEX_INPUT_DIR = "-vertexInputDir"
  final val EDGE_INPUT_DIR = "-edgeInputDir"
  final val GRAPH_OUTPUT_DIR = "-graphOutputDir"

  def collectVertexArgs(args: Array[String]): VertexGeneratorArgs = {
    var execId: Option[String] = None
    var appConfPath: Option[String] = None
    var kernelConfPath: Option[String] = None
    var dataSourcePath: Option[String] = None
    var vertexOutputPath: Option[String] = None
    args.sliding(2, 1).toList.collect {
      case Array(EXEC_ID, arg) => execId = Some(arg)
      case Array(APP_CONF_PATH, arg) => appConfPath = Some(arg)
      case Array(KERNEL_CONF_PATH, arg) => kernelConfPath = Some(arg)
      case Array(DATA_SOURCE_DIR, arg) => dataSourcePath = Some(arg)
      case Array(VERTEX_OUTPUT_DIR, arg) => vertexOutputPath = Some(arg)
    }
    VertexGeneratorArgs(
      executionId = execId.getOrElse(throw ERKernelError(s"Missing args $EXEC_ID")),
      appConfigPath = appConfPath.getOrElse(throw ERKernelError(s"Missing args $APP_CONF_PATH")),
      kernelConfigPath = kernelConfPath.getOrElse(throw ERKernelError(s"Missing args $KERNEL_CONF_PATH")),
      dataSourcesDir = dataSourcePath.getOrElse(throw ERKernelError(s"Mising args $DATA_SOURCE_DIR ")),
      vertexOutputDir = vertexOutputPath.getOrElse(throw ERKernelError(s"Mising args $VERTEX_OUTPUT_DIR "))
    )
  }

  def collectEdgeArgs(args: Array[String]): EdgeGeneratorArgs = {
    var execId: Option[String] = None
    var appConfPath: Option[String] = None
    var kernelConfPath: Option[String] = None
    var dataSourcePath: Option[String] = None
    var vertexOutputPath: Option[String] = None
    args.sliding(2, 1).toList.collect {
      case Array(EXEC_ID, arg) => execId = Some(arg)
      case Array(APP_CONF_PATH, arg) => appConfPath = Some(arg)
      case Array(EDGE_CONF_PATH, arg) => kernelConfPath = Some(arg)
      case Array(VERTEX_OUTPUT_DIR, arg) => dataSourcePath = Some(arg)
      case Array(EDGE_OUTPUT_DIR, arg) => vertexOutputPath = Some(arg)
    }
    EdgeGeneratorArgs(
      executionId = execId.getOrElse(throw ERKernelError(s"Missing args $EXEC_ID")),
      appConfigPath = appConfPath.getOrElse(throw ERKernelError(s"Missing args $APP_CONF_PATH")),
      kernelConfigPath = kernelConfPath.getOrElse(throw ERKernelError(s"Missing args $EDGE_CONF_PATH")),
      vertexInputDir = dataSourcePath.getOrElse(throw ERKernelError(s"Mising args $DATA_SOURCE_DIR ")),
      edgeOutputDir = vertexOutputPath.getOrElse(throw ERKernelError(s"Mising args $EDGE_OUTPUT_DIR "))
    )
  }

  def collectGraphArgs(args: Array[String]): GraphGeneratorArgs = {
    var execId: Option[String] = None
    var appConfPath: Option[String] = None
    var kernelConfPath: Option[String] = None
    var vertexInputDir: Option[String] = None
    var edgeInputDir: Option[String] = None
    var graphOutputPath: Option[String] = None
    args.sliding(2, 1).toList.collect {
      case Array(EXEC_ID, arg) => execId = Some(arg)
      case Array(APP_CONF_PATH, arg) => appConfPath = Some(arg)
      case Array(EDGE_CONF_PATH, arg) => kernelConfPath = Some(arg)
      case Array(VERTEX_INPUT_DIR, arg) => vertexInputDir = Some(arg)
      case Array(EDGE_INPUT_DIR, arg) => edgeInputDir = Some(arg)
      case Array(GRAPH_OUTPUT_DIR, arg) => graphOutputPath = Some(arg)
    }
    GraphGeneratorArgs(
      executionId = execId.getOrElse(throw ERKernelError(s"Missing args $EXEC_ID")),
      appConfigPath = appConfPath.getOrElse(throw ERKernelError(s"Missing args $APP_CONF_PATH")),
      kernelConfigPath = kernelConfPath.getOrElse(throw ERKernelError(s"Missing args $EDGE_CONF_PATH")),
      vertexInputDir = vertexInputDir.getOrElse(throw ERKernelError(s"Mising args $VERTEX_INPUT_DIR ")),
      edgeInputDir = edgeInputDir.getOrElse(throw ERKernelError(s"Mising args $EDGE_INPUT_DIR ")),
      graphOutputPath = graphOutputPath.getOrElse(throw ERKernelError(s"Mising args $GRAPH_OUTPUT_DIR "))
    )
  }
}
