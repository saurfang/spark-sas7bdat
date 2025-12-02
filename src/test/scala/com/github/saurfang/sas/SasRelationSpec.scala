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

import com.github.saurfang.sas.parso.ParsoWrapper
import com.github.saurfang.sas.spark._
import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.scalactic.TolerantNumerics.tolerantDoubleEquality
import org.scalatest.{FlatSpec, Matchers}

class SasRelationSpec extends FlatSpec with Matchers with SharedSparkContext {

  "SASRelation" should "read basic numeric SAS data exactly correct" in {

    val sqlContext = new SQLContext(sc)
    implicit val dblEquality = tolerantDoubleEquality(ParsoWrapper.EPSILON)

    // Set configs to cause multiple partitions/splits.
    val conf = sqlContext.sparkContext.hadoopConfiguration
    conf.setLong("mapred.max.split.size", 64L * 1024 * 1024)

    // Get the path for our test files.
    val sasRandomPath = getClass.getResource("/random.sas7bdat").getPath
    val csvRandomPath = getClass.getResource("/random.csv").getPath

    // Read Data
    val sasRandomDF = sqlContext.sasFile(sasRandomPath)
    val csvRandomDF = {
      sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(csvRandomPath)
    }

    // Print the automatically inferred schema.
    sasRandomDF.printSchema()
    csvRandomDF.printSchema()

    // Ensure that we read the correct number of rows.
    sasRandomDF.count() should ===(1000000)

    val csvNormalized = csvRandomDF.select(csvRandomDF("x").cast(DoubleType).as("x"), csvRandomDF("f").cast(LongType).as("f"))
    val sasAgg = sasRandomDF.agg(sum("x"), sum("f")).first()
    val csvAgg = csvNormalized.agg(sum("x"), sum("f")).first()
    def toDouble(rowValue: Any): Double = rowValue.asInstanceOf[java.lang.Number].doubleValue()
    toDouble(sasAgg.get(0)) shouldBe (toDouble(csvAgg.get(0)) +- 1e-6)
    toDouble(sasAgg.get(1)) shouldBe (toDouble(csvAgg.get(1)) +- 1e-6)
  }

  "SASRelation" should "export datetime SAS data to csv/parquet" in {

    val sqlContext = new SQLContext(sc)

    // Get the path for our test file.
    val datetimeFilePath = getClass.getResource("/datetime.sas7bdat").getPath

    // Read SAS file using implicit reader.
    val datetimeDF = sqlContext.sasFile(datetimeFilePath)

    // Print the automatically inferred schema.
    datetimeDF.printSchema()

    // Test writing SAS data to parquet.
    datetimeDF.write.mode("overwrite").format("parquet").save(getClass.getResource("/").getPath + "datetime.parquet")

    // Test writing SAS data to CSV.
    datetimeDF.write.mode("overwrite").format("csv").option("header", "true")
      .save(getClass.getResource("/").getPath + "datetime.csv")
  }

  "SASRelation" should "read internally compressed SAS data exactly correct" in {

    val sqlContext = new SQLContext(sc)

    // Set configs to cause multiple partitions/splits.
    val conf = sqlContext.sparkContext.hadoopConfiguration
    conf.setLong("mapred.max.split.size", 64L * 1024 * 1024)

    // Get the path for our test files.
    val sasRecapturePath = getClass.getResource("/recapture_test_compressed.sas7bdat").getPath
    val csvRecapturePath = getClass.getResource("/recapture_test_compressed.csv").getPath

    // Specify Schema
    val struct = StructType(
      StructField("Date", DateType, true) ::
        StructField("Week", DoubleType, true) ::
        StructField("Month", DoubleType, true) ::
        StructField("Day_of_Month", DoubleType, true) ::
        StructField("Weekday", StringType, true) ::
        StructField("Quarter", DoubleType, true) ::
        StructField("Year", DoubleType, true) ::
        StructField("Agency", StringType, true) ::
        StructField("Separation_Reason", StringType, true) ::
        StructField("Loan_Type", StringType, true) ::
        StructField("State_Type", StringType, true) ::
        StructField("Voluntary_Separation_Units", DoubleType, true) ::
        StructField("Assigned_Units", DoubleType, true) ::
        StructField("HDL_Assigned_Units", DoubleType, true) ::
        StructField("Marketed_Units", DoubleType, true) ::
        StructField("Contact_Units", DoubleType, true) ::
        StructField("HDL_Call_Units", DoubleType, true) ::
        StructField("App_Units", DoubleType, true) ::
        StructField("Lock_Units", DoubleType, true) ::
        StructField("Fund_Units", DoubleType, true) ::
        StructField("GNMA_Fund_Units", DoubleType, true) ::
        StructField("Voluntary_Separation_Balances", DoubleType, true) ::
        StructField("Assigned_Balances", DoubleType, true) ::
        StructField("HDL_Assigned_Balances", DoubleType, true) ::
        StructField("Marketed_Balances", DoubleType, true) ::
        StructField("Contact_Balances", DoubleType, true) ::
        StructField("HDL_Call_Balances", DoubleType, true) ::
        StructField("App_Balances", DoubleType, true) ::
        StructField("Lock_Balances", DoubleType, true) ::
        StructField("Fund_Balances", DoubleType, true) ::
        StructField("GNMA_Fund_Balances", DoubleType, true) ::
        StructField("Origination_Balances", DoubleType, true) ::
        StructField("id", DoubleType, true) :: Nil
    )


    // Read files.
    val sasRecaptureDF = sqlContext.sasFile(sasRecapturePath)
    val csvRecaptureDF = {
      sqlContext.read
        .format("csv")
        .option("header", true)
        .schema(struct)
        .load(csvRecapturePath)
    }

    // Print the automatically inferred schema.
    sasRecaptureDF.printSchema()
    csvRecaptureDF.printSchema()

    // Ensure that we read the correct number of rows.
    sasRecaptureDF.count() should ===(98366)

    // Ensure the schema was inferred correctly.
    sasRecaptureDF.schema should ===(csvRecaptureDF.schema)

    sasRecaptureDF.except(csvRecaptureDF).count() should ===(0)
    csvRecaptureDF.except(sasRecaptureDF).count() should ===(0)

  }

  "SASRelation" should "read SAS files with formatted numeric columns as decimals/longs if inferDecimal and inferLong are enabled" in {

    val sqlContext = new SQLContext(sc)

    // Get the path for our test files.
    val sasDataPath = getClass.getResource("/ag121a_supp_sample.sas7bdat").getPath
    val csvDataPath = getClass.getResource("/ag121a_supp_sample.csv").getPath

    // Specify Schema
    val struct = StructType(
      StructField("SURVYEAR", StringType, true) ::
        StructField("LEAID", StringType, true) ::
        StructField("NAME", StringType, true) ::
        StructField("PHONE", StringType, true) ::
        StructField("LATCOD", DecimalType(9, 6), true) ::
        StructField("LONCOD", DecimalType(11, 6), true) ::
        StructField("PKTCH", DecimalType(8, 2), true) ::
        StructField("AMPKM", LongType, true) :: Nil
    )

    // Read files
    val sasDataDF = {
      sqlContext.read
        .format("com.github.saurfang.sas.spark")
        .option("inferDecimal", true)
        .option("inferLong", true)
        .load(sasDataPath)
    }
    val csvDataDF = {
      sqlContext.read
        .format("csv")
        .option("header", true)
        .schema(struct)
        .load(csvDataPath)
    }

    // Print the automatically inferred schema.
    sasDataDF.printSchema()
    csvDataDF.printSchema()

    // Ensure the schema was inferred correctly.
    sasDataDF.schema should ===(csvDataDF.schema)

    sasDataDF.except(csvDataDF).count() should ===(0)
    csvDataDF.except(sasDataDF).count() should ===(0)
  }

  "SASRelation" should "read externally compressed numeric SAS data exactly correct" in {

    val sqlContext = new SQLContext(sc)
    implicit val dblEquality = tolerantDoubleEquality(ParsoWrapper.EPSILON)

    // Set configs to cause multiple partitions/splits.
    val conf = sqlContext.sparkContext.hadoopConfiguration

    // Get the path for our test files.
    val sasRandomPath = getClass.getResource("/random.sas7bdat.gz").getPath
    val csvRandomPath = getClass.getResource("/random.csv").getPath

    // Read Data
    val sasRandomDF = sqlContext.sasFile(sasRandomPath)
    val csvRandomDF = {
      sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(csvRandomPath)
    }

    // Print the automatically inferred schema.
    sasRandomDF.printSchema()
    csvRandomDF.printSchema()

    // Ensure that we read the correct number of rows.
    sasRandomDF.count() should ===(1000000)

    val csvNormalized = csvRandomDF.select(csvRandomDF("x").cast(DoubleType).as("x"), csvRandomDF("f").cast(LongType).as("f"))
    val sasAgg = sasRandomDF.agg(sum("x"), sum("f")).first()
    val csvAgg = csvNormalized.agg(sum("x"), sum("f")).first()
    def toDouble(rowValue: Any): Double = rowValue.asInstanceOf[java.lang.Number].doubleValue()
    toDouble(sasAgg.get(0)) shouldBe (toDouble(csvAgg.get(0)) +- 1e-6)
    toDouble(sasAgg.get(1)) shouldBe (toDouble(csvAgg.get(1)) +- 1e-6)
  }

  "SASRelation" should "read externally splittable compressed numeric SAS data exactly correct" in {

    val sqlContext = new SQLContext(sc)
    implicit val dblEquality = tolerantDoubleEquality(ParsoWrapper.EPSILON)

    // Set configs to cause multiple partitions/splits.
    val conf = sqlContext.sparkContext.hadoopConfiguration
    conf.setLong("mapred.max.split.size", 64L * 1024 * 1024)

    // Get the path for our test files.
    val sasRandomPath = getClass.getResource("/random.sas7bdat.bz2").getPath
    val csvRandomPath = getClass.getResource("/random.csv").getPath

    // Read Data
    val sasRandomDF = sqlContext.sasFile(sasRandomPath)
    val csvRandomDF = {
      sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(csvRandomPath)
    }

    // Print the automatically inferred schema.
    sasRandomDF.printSchema()
    csvRandomDF.printSchema()

    // Ensure that we read the correct number of rows.
    sasRandomDF.count() should ===(1000000)

    val csvNormalized = csvRandomDF.select(csvRandomDF("x").cast(DoubleType).as("x"), csvRandomDF("f").cast(LongType).as("f"))
    val sasAgg = sasRandomDF.agg(sum("x"), sum("f")).first()
    val csvAgg = csvNormalized.agg(sum("x"), sum("f")).first()
    def toDouble(rowValue: Any): Double = rowValue.asInstanceOf[java.lang.Number].doubleValue()
    toDouble(sasAgg.get(0)) shouldBe (toDouble(csvAgg.get(0)) +- 1e-6)
    toDouble(sasAgg.get(1)) shouldBe (toDouble(csvAgg.get(1)) +- 1e-6)
  }

  "SASRelation" should "read externally compressed and internally compressed numeric SAS data exactly correct" in {

    val sqlContext = new SQLContext(sc)

    // Set configs to cause multiple partitions/splits.
    val conf = sqlContext.sparkContext.hadoopConfiguration

    // Get the path for our test files.
    val sasRecapturePath = getClass.getResource("/recapture_test_compressed.sas7bdat.gz").getPath
    val csvRecapturePath = getClass.getResource("/recapture_test_compressed.csv").getPath

    // Specify Schema
    val struct = StructType(
      StructField("Date", DateType, true) ::
        StructField("Week", DoubleType, true) ::
        StructField("Month", DoubleType, true) ::
        StructField("Day_of_Month", DoubleType, true) ::
        StructField("Weekday", StringType, true) ::
        StructField("Quarter", DoubleType, true) ::
        StructField("Year", DoubleType, true) ::
        StructField("Agency", StringType, true) ::
        StructField("Separation_Reason", StringType, true) ::
        StructField("Loan_Type", StringType, true) ::
        StructField("State_Type", StringType, true) ::
        StructField("Voluntary_Separation_Units", DoubleType, true) ::
        StructField("Assigned_Units", DoubleType, true) ::
        StructField("HDL_Assigned_Units", DoubleType, true) ::
        StructField("Marketed_Units", DoubleType, true) ::
        StructField("Contact_Units", DoubleType, true) ::
        StructField("HDL_Call_Units", DoubleType, true) ::
        StructField("App_Units", DoubleType, true) ::
        StructField("Lock_Units", DoubleType, true) ::
        StructField("Fund_Units", DoubleType, true) ::
        StructField("GNMA_Fund_Units", DoubleType, true) ::
        StructField("Voluntary_Separation_Balances", DoubleType, true) ::
        StructField("Assigned_Balances", DoubleType, true) ::
        StructField("HDL_Assigned_Balances", DoubleType, true) ::
        StructField("Marketed_Balances", DoubleType, true) ::
        StructField("Contact_Balances", DoubleType, true) ::
        StructField("HDL_Call_Balances", DoubleType, true) ::
        StructField("App_Balances", DoubleType, true) ::
        StructField("Lock_Balances", DoubleType, true) ::
        StructField("Fund_Balances", DoubleType, true) ::
        StructField("GNMA_Fund_Balances", DoubleType, true) ::
        StructField("Origination_Balances", DoubleType, true) ::
        StructField("id", DoubleType, true) :: Nil
    )


    // Read files.
    val sasRecaptureDF = sqlContext.sasFile(sasRecapturePath)
    val csvRecaptureDF = {
      sqlContext.read
        .format("csv")
        .option("header", true)
        .schema(struct)
        .load(csvRecapturePath)
    }

    // Print the automatically inferred schema.
    sasRecaptureDF.printSchema()
    csvRecaptureDF.printSchema()

    // Ensure that we read the correct number of rows.
    sasRecaptureDF.count() should ===(98366)

    // Ensure the schema was inferred correctly.
    sasRecaptureDF.schema should ===(csvRecaptureDF.schema)

    sasRecaptureDF.except(csvRecaptureDF).count() should ===(0)
    csvRecaptureDF.except(sasRecaptureDF).count() should ===(0)
  }
}
