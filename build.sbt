name := "denormalizer"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.4.6"

libraryDependencies += "io.delta" %% "delta-core" % "0.7.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0"
libraryDependencies += "joda-time" % "joda-time" % "2.10.8"
libraryDependencies += "com.scylladb" % "scylla-driver-core" % "3.10.1-scylla-0"
libraryDependencies += "com.scylladb" % "scylla-driver-mapping" % "3.10.1-scylla-0"
libraryDependencies += "com.scylladb" % "scylla-driver-extras" % "3.10.1-scylla-0"

libraryDependencies += "com.typesafe" % "config" % "1.4.1"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.10.6"
//libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "8.2.2.jre8"