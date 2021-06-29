package org.wumiguo.erkernel.common

import org.slf4j.{Logger, LoggerFactory}

/**
 * @author levin 
 *         Created on 2021/6/21
 */
trait Loggable {
  final val LOG: Logger = LoggerFactory.getLogger(getClass)
}
