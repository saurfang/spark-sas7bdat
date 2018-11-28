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

package com.github.saurfang.sas.util

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
  * Export sas7bdat file to parquet/csv.
  * First argument is the input file. Second argument is the output path.
  * Output type is determined by the extension (.csv or .parquet)
  */
object SasExport {

  def main(args: Array[String]): Unit = {
    val log = LogManager.getLogger(this.getClass.getName)
    log.info(args.mkString(" "))

    val spark = SparkSession.builder.appName("Spark sas7bdat").getOrCreate()
    val df = spark.read.format("com.github.saurfang.sas.spark").load(args(0))

    val output = args(1)
    if (output.endsWith(".csv")) {
      df.write.format("csv").option("header", "true").save(output)
    } else if (output.endsWith(".parquet")) {
      df.write.parquet(output)
    }
  }
}
