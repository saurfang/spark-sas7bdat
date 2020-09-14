## 3.0.0

- **Changed**: Upgrade Parso to 2.0.11.
- **Changed**: Support Spark 3.x / Scala 2.12.x. (#59 Thanks @thesuperzapper)
- **Added**: Support for externally compressed sas7bdat files. (#50 Thanks @thadeusb)

## 2.1.0

- **Changed**: Upgrade Parso to 2.0.10.
- **Fixed**: Incorrect reading of internally compressed SAS files. (See: #38 Thanks @thesuperzapper)
- **Added**: Extraction of SAS column labels as comments for Parquet/Hive.
- **Added**: Ability to force SAS column names to lowercase.
- **Added**: Schema inference for Long, Decimal, Int, Float, Short. (Based on SAS column formatters)
- **Added**: Ability to specify the maximum/minimum size of input splits.
- **Added**: Timeout for schema inference (handles corrupt files)
- **Improved**: Non-valid SAS files now throw errors on schema inference.
- **Improved**: User provided schema is now honoured, if conversion to the provided type is possible.
- **Removed**: Ability to provide jobConf in the implicit reader. (Instead use `SparkContext.hadoopConfiguration`)

## 2.0.0

- **Changed**: Upgrade Parso to 2.0.8. (Thanks @mulya, @Tagar, @printsev)
- **Changed**: Package is now licensed under Apache License 2.0.

## 1.1.5

- **Changed**: Package now compiles against Spark 2.0.1. (Thanks @chappers)

## 1.1.4

- **Fixed**: Remove dependency on GenericMutableRow so package works for 1.4 and above.
- **Added**: Add date datatype support and Date will now return as `java.sql.Date`.
- **Changed**: Package now compiles against 1.5.0.

## 1.1.3

- **Added**: Add proper support for load via sqlContext.

## 1.1.2

- **Fixed**: Force javac version to 1.7 for better runtime compatibility.

## 1.1.1

- **Fixed**: Override parso sas7bdat parser to fill data page fully during read. This fixes exceptions
  thrown when reading data from AWS S3n.

## 1.1.0

- **Changed**: Migrate Hadoop 2.x (mapreduce) implementation to Hadoop 1.x (mapred) for better compatibility.
  As a result, the library is now runnable with all Hadoop versions.
