package org.wumiguo.erkernel.model

/**
 * GraphResultSetting describes the final graph output which is the joint result of multi-datasources.
 *
 * @author levin 
 *         Created on 2021/6/25
 */
case class GraphResultSetting(
                               graphId: String,
                               tripletsOutputName: String,
                               vertexOutputName: String,
                               connectedComponentAttrName: String,
                               relationshipInclusion: List[String],
                               relationshipExclusion: List[String]
                             )
