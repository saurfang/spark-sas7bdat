name := "spark-sas7bdat"
version := "3.0.0"
organization := "com.github.saurfang"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalaVersion := "2.12.11"
crossScalaVersions := Seq("2.11.12", "2.12.11")

scalacOptions ++= Seq("-target:jvm-1.8")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  // main
  "com.epam" % "parso" % "2.0.11",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",

  // testing
  "org.scalatest" %% "scalatest" % "3.1.2" % "test",
)

//sbt-spark-package
spName := "saurfang/spark-sas7bdat"
sparkVersion := sys.props.getOrElse("spark.version", if (scalaVersion.value.startsWith("2.11")) "2.4.6" else "3.0.0")
sparkComponents += "sql"
spAppendScalaVersion := true
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

//include provided dependencies in sbt run task
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

//only one living spark-context is allowed
parallelExecution in Test := false

//set java options for consistent timestamp test
javaOptions in Test += "-Duser.timezone=UTC"
fork in Test := true

//skip test during assembly
test in assembly := {}
