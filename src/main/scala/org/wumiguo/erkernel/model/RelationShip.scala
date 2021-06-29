package org.wumiguo.erkernel.model

/**
 * RelationShip is an edge to connect 2 entity entry points(vertices)
 *
 * @author levin 
 *         Created on 2021/6/25
 */
case class RelationShip(
                         relationId: String,
                         subject: String,
                         counterParty: String,
                         relationShipLabel: String,
                         relationLogic: String
                       )
