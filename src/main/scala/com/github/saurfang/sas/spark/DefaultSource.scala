// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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