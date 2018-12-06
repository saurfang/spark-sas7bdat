## 2.1.0

- __Changed__: Upgrade Parso to 2.0.10.
- __Fixed__: Incorrect reading of internally compressed SAS files. (See: #38)
- __Added__: Extraction of SAS column labels as comments for Parquet/Hive.
- __Added__: Ability to force SAS column names to lowercase.
- __Added__: Schema inference for Long, Decimal, Int, Float, Short. (Based on SAS column formatters)
- __Added__: Ability to specify the maximum/minimum size of input splits.
- __Added__: Timeout for schema inference (handles corrupt files)
- __Improved__: Non-valid SAS files now throw errors on schema inference.
- __Improved__: User provided schema is now honoured, if conversion to the provided type is possible.
- __Removed__: Ability to provide jobConf in the implicit reader. (Instead use `SparkContext.hadoopConfiguration`)

## 2.0.0

- __Changed__: Upgrade Parso to 2.0.8. (Thanks @mulya, @Tagar, @printsev)
- __Changed__: Package is now licensed under Apache License 2.0.

## 1.1.5

- __Changed__: Package now compiles against Spark 2.0.1. (Thanks @chappers)

## 1.1.4

- __Fixed__: Remove dependency on GenericMutableRow so package works for 1.4 and above.
- __Added__: Add date datatype support and Date will now return as `java.sql.Date`.
- __Changed__: Package now compiles against 1.5.0.

## 1.1.3

- __Added__: Add proper support for load via sqlContext.

## 1.1.2

- __Fixed__: Force javac version to 1.7 for better runtime compatibility.

## 1.1.1

- __Fixed__: Override parso sas7bdat parser to fill data page fully during read. This fixes exceptions
thrown when reading data from AWS S3n.

## 1.1.0

- __Changed__: Migrate Hadoop 2.x (mapreduce) implementation to Hadoop 1.x (mapred) for better compatibility.
As a result, the library is now runnable with all Hadoop versions.
