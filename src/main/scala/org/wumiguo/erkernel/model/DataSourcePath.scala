package org.wumiguo.erkernel.model

/**
 * This is to represent a data source with partitioned data inside a folder
 *
 * @author levin 
 *         Created on 2021/6/23
 */
case class DataSourcePath(
                           basePath: String,
                           partitionPaths: Seq[String]
                         )
