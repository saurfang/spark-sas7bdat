# Spark SQL SAS Library

A library for parsing SAS data (.sas7bdat) with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).
This also includes a `SasInputFormat` designed for Hadoop mapreduce. This format is splittable when input is uncompressed 
thus can achieve high parallelism for a large SAS file.

This library is inspired by [spark-csv](https://github.com/databricks/spark-csv) and 
currently uses [parso](http://scitouch.net/opensource/parso) for parsing as it is the only public available parser
that handles both forms of SAS compression (CHAR and Binary). 

[![Build Status](https://travis-ci.org/saurfang/spark-sas7bdat.svg?branch=master)](https://travis-ci.org/saurfang/spark-sas7bdat)

## Requirements

This library requires Spark 1.3+

## Features

This package allows reading SAS files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
Schema is automatically inferred from metainfo embedded in the SAS file.

### Scala API
The recommended way to load SAS data is using the load/save functions in SQLContext.

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load("sas.spark", Map("path" -> "cars.sas7bdat"))
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")
```

You can also use the implicits from `import sas.spark._`.

```scala
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv._
import sas.spark._

val sqlContext = new SQLContext(sc)

val cars = sqlContext.sasFile("cars.sas7bdat")
cars.select("year", "model").saveAsCsvFile("newcars.tsv")
```

### SAStoCSV Runner
We also included a simple `SAStoCSV` Spark program that converts .sas7bdat to .csv file:

```bash
sbt "run input.sas7bdat output.csv"
```

## Related Work

* [spark-csv](https://github.com/databricks/spark-csv)
* [parso](http://scitouch.net/opensource/parso)
* [haven](https://github.com/hadley/haven)
* [ReadStat](https://github.com/WizardMac/ReadStat)
* [SAS7BDAT Database Binary Format](http://www2.uaem.mx/r-mirror/web/packages/sas7bdat/vignettes/sas7bdat.pdf)
