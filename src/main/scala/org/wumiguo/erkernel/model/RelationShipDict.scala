package org.wumiguo.erkernel.model

/**
 * DataSourceDict can help user to manage the multiple entity RelationShip and for easy lookup by relationId.
 *
 * @author levin
 *         Created on 2021/6/22
 */
case class RelationShipDict(dict: Map[String, RelationShip])
