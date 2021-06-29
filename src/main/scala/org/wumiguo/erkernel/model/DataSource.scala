package org.wumiguo.erkernel.model

/**
 * DataSource is a source of entity data which is the vertices set to represent the entity in the world
 *
 * @author levin 
 *         Created on 2021/6/22
 */

case class DataSource(
                       sourceName: String,
                       partitions: List[String],
                       distinctBy: String,
                       filterTerm: String,
                       columnSpec: Seq[(String, String)]
                     )
