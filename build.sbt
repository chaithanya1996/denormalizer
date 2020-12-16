name := "denormalizer"

version := "0.2"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"

libraryDependencies += "io.delta" %% "delta-core" % "0.7.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0"
libraryDependencies += "joda-time" % "joda-time" % "2.10.8"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.0"

libraryDependencies += "com.google.guava" % "guava" % "30.0-jre"

libraryDependencies += "com.typesafe" % "config" % "1.4.1"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.10.6"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "8.2.2.jre11"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.10"



assemblyMergeStrategy in assembly := {
  case "mozilla/public-suffix-list.txt"                            => MergeStrategy.first
  case "module-info.class"                                => MergeStrategy.first
  case "META-INF/io.netty.versions.properties"            => MergeStrategy.first
  case PathList("javax", xs @ _*)         => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}