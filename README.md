# Spark SQL SAS Library

A library for parsing SAS data (.sas7bdat) with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).
This also includes a `SasInputFormat` designed for Hadoop mapreduce. This format is splittable when input is uncompressed 
thus can achieve high parallelism for a large SAS file.

This library is inspired by [spark-csv](https://github.com/databricks/spark-csv) and 
currently uses [parso](http://scitouch.net/opensource/parso) for parsing as it is the only public available parser
that handles both forms of SAS compression (CHAR and Binary). Note *parso* is licensed under GPL-3 and subsequently
this library is also licensed as such.

[![Build Status](https://travis-ci.org/saurfang/spark-sas7bdat.svg?branch=master)](https://travis-ci.org/saurfang/spark-sas7bdat)

## Requirements

This library requires Spark 1.3+

## How To Use

This package is published using [sbt-spark-package](https://github.com/databricks/sbt-spark-package) and 
linking information can be found at http://spark-packages.org/package/saurfang/spark-sas7bdat

## Features

This package allows reading SAS files in local or distributed filesystem as 
[Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).

Schema is automatically inferred from meta information embedded in the SAS file.

Thanks to the splittable `SasInputFormat`, we are able to convert a 200GB (1.5Bn rows) .sas7bdat file 
to .csv files using 2000 executors in under 2 minutes.

### Scala API
The recommended way to load SAS data is using the load functions in SQLContext.

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load("com.github.saurfang.sas.sas.spark", Map("path" -> "cars.sas7bdat"))
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")
```

You can also use the implicits from `import com.github.saurfang.sas.sas.spark._`.

```scala
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv._
import com.github.saurfang.sas.sas.spark._

val sqlContext = new SQLContext(sc)

val cars = sqlContext.sasFile("cars.sas7bdat")
cars.select("year", "model").saveAsCsvFile("newcars.tsv")
```

## SAS Export Runner
We also included a simple `SasExport` Spark program that converts *.sas7bdat* to *.csv* or *.parquet* file:

```bash
sbt "run input.sas7bdat output.csv"
sbt "run input.sas7bdat output.parquet"
```

### Caveats

1. `spark-csv` writes out `null` as "null" in csv text output. This means if you read it back for a string type,
you might actually read "null" instead of `null`. The safest option is to export in parquet format where
null is properly recorded.
2. The version of parquet used in SparkSQL 1.3.0 don't support date type. Hence date are coerced into
string with yyyy-MM-dd format.

## Related Work

* [spark-csv](https://github.com/databricks/spark-csv)
* [parso](http://scitouch.net/opensource/parso)
* [ReadStat](https://github.com/WizardMac/ReadStat)
* [SAS7BDAT Database Binary Format](http://www2.uaem.mx/r-mirror/web/packages/sas7bdat/vignettes/sas7bdat.pdf)
