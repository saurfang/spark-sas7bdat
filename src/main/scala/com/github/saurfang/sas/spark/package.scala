// Copyright (C) 2015 Forest Fang.
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

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SQLContext}

package object spark {

  /**
   * Adds a method, `sasFile`, to SQLContext that allows reading SAS data.
   */
  implicit class SasContext(sqlContext: SQLContext) {
    def sasFile(filePath: String,
                conf: JobConf = new JobConf()): DataFrame = {
      val sasRelation = SasRelation(
        location = filePath,
        conf = conf)(sqlContext)
      sqlContext.baseRelationToDataFrame(sasRelation)
    }
  }

}
