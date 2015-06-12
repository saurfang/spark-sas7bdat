## 1.1.2

- __Fixed__: Force javac version to 1.7 for better runtime compatibility.

## 1.1.1

- __Fixed__: Override parso sas7bdat parser to fill data page fully during read. This fixes exceptions
thrown when reading data from AWS S3n.

## 1.1.0

- __Changed__: Migrate Hadoop 2.x (mapreduce) implementation to Hadoop 1.x (mapred) for better compatibility.
As a result, the library is now runnable with all Hadoop versions.
