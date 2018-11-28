// Copyright (C) 2018 Forest Fang.
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
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]
                             ): SasRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
    * Creates a new relation for data store in sas7bdat given parameters and user supported schema.
    * Parameters have to include 'path'
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType
                             ): SasRelation = {

    val path = checkPath(parameters)

    // extractLabel
    val extractLabel = parameters.getOrElse("extractLabel", "false")
    val extractLabelFlag = {
      if (extractLabel == "false") {
        false
      } else if (extractLabel == "true") {
        true
      } else {
        throw new Exception("extractLabel can only be true or false")
      }
    }

    // forceLowercaseNames
    val forceLowercaseNames = parameters.getOrElse("forceLowercaseNames", "false")
    val forceLowercaseNamesFlag = {
      if (forceLowercaseNames == "false") {
        false
      } else if (forceLowercaseNames == "true") {
        true
      } else {
        throw new Exception("forceLowercaseNames can only be true or false")
      }
    }

    // inferDecimal
    val inferDecimal = parameters.getOrElse("inferDecimal", "false")
    val inferDecimalFlag = {
      if (inferDecimal == "false") {
        false
      } else if (inferDecimal == "true") {
        true
      } else {
        throw new Exception("inferDecimal can only be true or false")
      }
    }

    // inferDecimalScale
    val inferDecimalScaleStr = parameters.getOrElse("inferDecimalScale", "")
    val inferDecimalScaleInt: Option[Int] = {
      try {
        if (inferDecimalScaleStr.isEmpty) {
          None
        } else {
          val decimalScale = inferDecimalScaleStr.toInt
          require(decimalScale > 0 && decimalScale <= 38)
          Some(decimalScale)
        }
      } catch {
        case e: Exception => throw new Exception("inferDecimalScale must be a valid integer between 1 and 38")
      }
    }

    // inferFloat
    val inferFloat = parameters.getOrElse("inferFloat", "false")
    val inferFloatFlag = {
      if (inferFloat == "false") {
        false
      } else if (inferFloat == "true") {
        true
      } else {
        throw new Exception("inferFloat can only be true or false")
      }
    }

    // inferInt
    val inferInt = parameters.getOrElse("inferInt", "false")
    val inferIntFlag = {
      if (inferInt == "false") {
        false
      } else if (inferInt == "true") {
        true
      } else {
        throw new Exception("inferInt can only be true or false")
      }
    }

    // inferLong
    val inferLong = parameters.getOrElse("inferLong", "false")
    val inferLongFlag = {
      if (inferLong == "false") {
        false
      } else if (inferLong == "true") {
        true
      } else {
        throw new Exception("inferLong can only be true or false")
      }
    }

    // inferShort
    val inferShort = parameters.getOrElse("inferShort", "false")
    val inferShortFlag = {
      if (inferShort == "false") {
        false
      } else if (inferShort == "true") {
        true
      } else {
        throw new Exception("inferShort can only be true or false")
      }
    }

    // metadataTimeout
    val metadataTimeoutStr = parameters.getOrElse("metadataTimeout", "60")
    val metadataTimeoutInt = {
      try {
        metadataTimeoutStr.toInt
      } catch {
        case e: Exception => throw new Exception("metadataTimeout must be a valid integer")
      }
    }

    // minSplitSize
    val minSplitSizeStr = parameters.getOrElse("minSplitSize", "")
    val minSplitSizeLong: Option[Long] = {
      try {
        if (minSplitSizeStr.isEmpty) {
          None
        } else {
          Some(minSplitSizeStr.toLong)
        }
      } catch {
        case e: Exception => throw new Exception("minSplitSize must be a valid long")
      }
    }

    // maxSplitSize
    val maxSplitSizeStr = parameters.getOrElse("maxSplitSize", "")
    val maxSplitSizeLong: Option[Long] = {
      try {
        if (maxSplitSizeStr.isEmpty) {
          None
        } else {
          Some(maxSplitSizeStr.toLong)
        }
      } catch {
        case e: Exception => throw new Exception("maxSplitSize must be a valid long")
      }
    }

    SasRelation(
      location = path,
      userSchema = schema,
      extractLabel = extractLabelFlag,
      forceLowercaseNames = forceLowercaseNamesFlag,
      inferDecimal = inferDecimalFlag,
      inferDecimalScale = inferDecimalScaleInt,
      inferFloat = inferFloatFlag,
      inferInt = inferIntFlag,
      inferLong = inferLongFlag,
      inferShort = inferShortFlag,
      metadataTimeout = metadataTimeoutInt,
      minSplitSize = minSplitSizeLong,
      maxSplitSize = maxSplitSizeLong
    )(sqlContext)
  }

}
