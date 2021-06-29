package org.wumiguo.erkernel.model

/**
 * DataSourceDict can help user to manage the multiple entity data source and for easy lookup by dataSource name.
 *
 * @author levin
 *         Created on 2021/6/22
 */
case class DataSourceDict(dict: Map[String, DataSource])
