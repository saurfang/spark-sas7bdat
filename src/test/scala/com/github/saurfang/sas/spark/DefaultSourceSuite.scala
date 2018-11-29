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

import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSuite, Matchers}

class DefaultSourceSuite extends FunSuite with Matchers with SharedSparkContext {

  test("Data loaded via Implicit SQLContext should be the same as that from SQL Statements.") {

    val sqlContext = new SQLContext(sc)

    // Get the path for our test file.
    val sasDatetimePath = getClass.getResource("/datetime.sas7bdat").getPath

    // Read using implicit reader.
    val implicitDF = sqlContext.sasFile(sasDatetimePath)

    // Read using pure SQL statements.
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE datetimeTable
         |USING com.github.saurfang.sas.spark
         |OPTIONS (path "$sasDatetimePath")
      """.stripMargin)
    val sqlDF = sqlContext.sql("SELECT * FROM datetimeTable")

    // Ensure they return the same data.
    implicitDF.collect() should ===(sqlDF.collect())
  }

  test("Data loaded via Implicit SQLContext should be the same as that from default source specification.") {

    val sqlContext = new SQLContext(sc)

    // Get the path for our test file.
    val sasDatetimePath = getClass.getResource("/datetime.sas7bdat").getPath

    // Read using implicit reader.
    val implicitDF = sqlContext.sasFile(sasDatetimePath)

    // Read using default source.
    val directDF = sqlContext.read.format("com.github.saurfang.sas.spark").load(sasDatetimePath)

    // Ensure they return the same data.
    implicitDF.collect() should ===(directDF.collect())
  }

  test("Data loaded via Implicit DataFrameReader should be the same as that from default source specification.") {

    val sqlContext = new SQLContext(sc)

    // Get the path for our test file.
    val sasDatetimePath = getClass.getResource("/datetime.sas7bdat").getPath

    // Read using implicit reader.
    val implicitDF = sqlContext.read.sas(sasDatetimePath)

    // Read using default source.
    val directDF = sqlContext.read.format("com.github.saurfang.sas.spark").load(sasDatetimePath)

    // Ensure they return the same data.
    implicitDF.collect() should ===(directDF.collect())
  }
}
