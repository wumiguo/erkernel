package org.wumiguo.erkernel.model

/**
 * @author levin 
 *         Created on 2021/7/1
 */
case class TripleModel(idFrom: Long,
                       keyFrom: String,
                       nameFrom: String,
                       groupFrom: String,
                       idTo: Long,
                       keyTo: String,
                       nameTo: String,
                       groupTo: String,
                       edgeProperty: String,
                       relationShip: String)
