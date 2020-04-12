name := "SG"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"
//libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.12.3"