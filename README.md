# Spark SAS Data Source (sas7bdat)

A library for reading SAS data (.sas7bdat) with [Spark](http://spark.apache.org/). 



[![Build Status](https://travis-ci.org/saurfang/spark-sas7bdat.svg?branch=master)](https://travis-ci.org/saurfang/spark-sas7bdat) [![Join the chat at https://gitter.im/saurfang/spark-sas7bdat](https://badges.gitter.im/saurfang/spark-sas7bdat.svg)](https://gitter.im/saurfang/spark-sas7bdat?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Requirements:
* [Spark 1.4+ or 2.0+](https://spark.apache.org/downloads.html)
* [Parso 2.0.10](https://mvnrepository.com/artifact/com.epam/parso/2.0.10)

## Download:

The latest jar can be downloaded from [spark-packages](http://spark-packages.org/package/saurfang/spark-sas7bdat).

## Features:

* This package allows reading SAS files from local and distributed filesystems, into Spark DataFrames.
* Schema is automatically inferred from metadata embedded in the SAS file. _(Behaviour can be customised, see parameters below)_
* The SAS format is splittable when not file-system compressed, thus we are able to convert a 200GB (1.5Bn rows) .sas7bdat file to .csv files using 2000 executors in under 2 minutes.
* This library uses [parso](https://github.com/epam/parso/) for parsing as it is the only public available parser
that handles both forms of SAS compression (CHAR and BINARY). 

__NOTE:__ we currently only support reading
           
## Docs:

### Parameters:
* `extractLabel` _(Default: false)_
  * _Boolean:_ extract column labels as column comments for Parquet/Hive
* `forceLowercaseNames` _(Default: false)_
  * Boolean: force column names to lower case
* `inferDecimal` _(Default: false)_
  * Boolean: infer numeric columns (with format width >0 and format precision >0), as _Decimal(Width, Precision)_
* `inferDecimalScale` _(Default: each column's format width)_
  * Int: scale of inferred decimals
* `inferFloat` _(Default: false)_
  * Boolean: infer numeric columns (with <=4 bytes), as _Float_
* `inferInt` _(Default: false)_
  * Boolean: infer numeric columns (with <=4 bytes, format width >0 and format precision =0), as _Int_
* `inferLong` _(Default: false)_
  * Boolean: infer numeric columns (with <=8 bytes, format width >0 and format precision =0), as _Long_
* `inferShort` _(Default: false)_
  * Boolean: infer numeric columns (with <=2 bytes, format width >0 and format precision =0), as _Short_
* `metadataTimeout` _(Default: 60)_
  * Int: number of seconds to allow reading of file metadata _(stops corrupt files hanging)_
* `minPartitions` _(Default: 0)_
  * Int: minimum number splits to use when reading
  
__NOTE:__ the order of precedence for numeric type inference is: _Long_ -> _Int_ -> _Short_ -> _Decimal_ -> _Float_ -> _Double_

### Scala API
```scala
val df = {
  spark.read
    .format("com.github.saurfang.sas.spark")
    .option("forceLowercaseNames", true)
    .option("inferLong", true)
    .load("cars.sas7bdat")
}
df.write.format("csv").option("header", "true").save("newcars.csv")
```

You can also use the implicit reader: 

```scala
import com.github.saurfang.sas.spark._
val df = sqlContext.sasFile("cars.sas7bdat")
df.write.format("csv").option("header", "true").save("newcars.csv")
```
(_Note: you cannot use parameters like `inferLong` with the implicit reader._)
### Python API

```python
df = spark.read.format("com.github.saurfang.sas.spark").load("cars.sas7bdat", forceLowercaseNames=True, inferLong=True)
df.write.csv("newcars.csv", header=True)
```

### R API

```r
df <- read.df("cars.sas7bdat", source = "com.github.saurfang.sas.spark", forceLowercaseNames = TRUE, inferLong = TRUE)
write.df(df, path = "newcars.csv", source = "csv", header = TRUE)
```

### SQL API
SAS data can be queried in pure SQL by registering the data as a (temporary) table.

```sql
CREATE TEMPORARY TABLE cars
USING com.github.saurfang.sas.spark
OPTIONS (path "cars.sas7bdat")
```

### SAS Export Runner
We included a simple `SasExport` Spark program that converts _.sas7bdat_ to _.csv_ or _.parquet_ files:

```bash
sbt "run input.sas7bdat output.csv"
sbt "run input.sas7bdat output.parquet"
```

To achieve more parallelism, use `spark-submit` script to run it on a Spark cluster. If you don't have a spark
cluster, you can always run it in local mode and take advantage of multi-core.

### Spark Shell

```bash
spark-shell --master local[4] --packages saurfang:spark-sas7bdat:2.1.0-s_2.11
```

## Caveats

1. `spark-csv` writes out `null` as "null" in csv text output. This means if you read it back for a string type,
you might actually read "null" instead of `null`. The safest option is to export in parquet format where
null is properly recorded. See https://github.com/databricks/spark-csv/pull/147 for alternative solution.

## Related Work

* [parso](https://github.com/epam/parso)
* [sas7bdat format](http://www2.uaem.mx/r-mirror/web/packages/sas7bdat/vignettes/sas7bdat.pdf)
* [ReadStat](https://github.com/WizardMac/ReadStat)
