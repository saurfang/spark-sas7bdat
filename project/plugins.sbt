addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

resolvers += "Sonatype" at "https://oss.sonatype.org/content/repositories/releases/org/scalastyle"
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.7-astraea.1")
