name := "spark-sas7bdat"

version := "1.1.1"

organization := "com.github.saurfang"

scalaVersion := "2.10.5"

//crossScalaVersions := Seq("2.10.5", "2.11.6")

libraryDependencies ++= Seq(
  "com.databricks" %% "spark-csv" % "1.0.3",
  "com.ggasoftware" % "parso" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

//sbt-spark-package
spName := "saurfang/spark-sas7bdat"

sparkVersion := "1.3.0"

sparkComponents += "sql"

//spDependencies += "databricks/spark-csv:1.0.3"

spAppendScalaVersion := true

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "GPL-3.0" -> url("http://opensource.org/licenses/GPL-3.0")

//include provided dependencies in sbt run task
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

//only one living spark-context is allowed
parallelExecution in Test := false

//skip test during assembly
test in assembly := {}

//override modified parser class
assemblyMergeStrategy in assembly := {
  case PathList("com", "ggasoftware", xs @ _*)         => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}