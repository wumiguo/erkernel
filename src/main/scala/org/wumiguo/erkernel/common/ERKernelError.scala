package org.wumiguo.erkernel.common

/**
 * @author levin 
 *         Created on 2021/6/22
 */
case class ERKernelError(msg: String) extends Exception(msg)
