name := "spark-sas7bdat"
version := "3.0.0"
organization := "com.github.saurfang"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalaVersion := "2.13.12"
crossScalaVersions := Seq("2.11.12", "2.12.11", "2.13.12")

lazy val sparkVersionValue = Def.setting[String] {
  sys.props.getOrElse("spark.version", scalaBinaryVersion.value match {
    case "2.11" => "2.4.6"
    case "2.12" => "3.0.0"
    case "2.13" => "3.5.0"
  })
}

scalacOptions ++= Seq("-target:jvm-1.8")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  // spark
  "org.apache.spark" %% "spark-core" % sparkVersionValue.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersionValue.value % "provided",

  // main
  "com.epam" % "parso" % "2.0.14",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",

  // testing
  "org.scalatest" %% "scalatest" % "3.1.2" % "test",
)

//include provided dependencies in sbt run task
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

//only one living spark-context is allowed
parallelExecution in Test := false

//set java options for consistent timestamp test
javaOptions in Test ++= {
  val common = Seq("-Duser.timezone=UTC")
  val moduleArgs =
    if (sys.props.get("java.specification.version").exists(_.split("\\.").headOption.exists(_.toInt >= 9))) {
      Seq(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
      )
    } else {
      Seq.empty
    }
  common ++ moduleArgs
}
fork in Test := true

//skip test during assembly
test in assembly := {}
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "versions", _ @ _*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x => (assemblyMergeStrategy in assembly).value(x)
}
