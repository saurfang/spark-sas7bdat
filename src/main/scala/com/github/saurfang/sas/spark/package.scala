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

package com.github.saurfang.sas

import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

package object spark {

  /**
    * Adds a method, `sasFile`, to SQLContext that allows you to read sas files.
    */
  implicit class SasContext(sqlContext: SQLContext) {
    def sasFile(filePath: String): DataFrame = {
      val sasRelation = SasRelation(location = filePath)(sqlContext)
      sqlContext.baseRelationToDataFrame(sasRelation)
    }
  }

  /**
    * Adds a method, `sas`, to DataFrameReader that allows you to read sas files.
    */
  implicit class SasDataFrameReader(reader: DataFrameReader) {
    def sas: String => DataFrame = reader.format("com.github.saurfang.sas.spark").load
  }

}
