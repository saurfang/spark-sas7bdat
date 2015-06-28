package com.github.saurfang.sas.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Provides access to SAS data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider with SchemaRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for .sas7bdat data."))
  }

  /**
   * Creates a new relation for data store in sas7bdat given parameters.
   * Parameters have to include 'path'
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in sas7bdat given parameters and user supported schema.
   * Parameters have to include 'path'
   */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType) = {
    SasRelation(checkPath(parameters), schema)(sqlContext)
  }
}