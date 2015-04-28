name := """spark-sas7bdat"""

version := "1.0"

scalaVersion := "2.10.5"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided",
  "com.ggasoftware" % "parso" % "1.2.1",
  "com.databricks" %% "spark-csv" % "1.0.3",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

parallelExecution in Test := false

test in assembly := {}
