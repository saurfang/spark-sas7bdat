name := "spark-sas7bdat"
version := "2.1.0"
organization := "com.github.saurfang"

scalaVersion := "2.11.12"
crossScalaVersions := Seq("2.10.7", "2.11.12")

scalacOptions ++= Seq("-target:jvm-1.8")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "com.epam" % "parso" % "2.0.10",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "2.7"
)

//sbt-spark-package
spName := "saurfang/spark-sas7bdat"
sparkVersion := "2.2.0"
sparkComponents += "sql"
spAppendScalaVersion := true
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

//include provided dependencies in sbt run task
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

//only one living spark-context is allowed
parallelExecution in Test := false

//set java options for consistent timestamp test
javaOptions in Test += "-Duser.timezone=UTC"
fork in Test := true

//skip test during assembly
test in assembly := {}
