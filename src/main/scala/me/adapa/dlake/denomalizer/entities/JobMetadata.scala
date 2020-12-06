package me.adapa.dlake.denomalizer.entities

import me.adapa.dlake.denomalizer.config.DestinationType.DestinationType
import me.adapa.dlake.denomalizer.config.SparkJobType.SparkJobType
import me.adapa.dlake.denomalizer.config.ProcessType.ProcessType
import me.adapa.dlake.denomalizer.config.SourceType.SourceType
import me.adapa.dlake.denomalizer.config.SparkJobType
import me.adapa.dlake.denomalizer.config.DestinationType
import me.adapa.dlake.denomalizer.config.ProcessType
import me.adapa.dlake.denomalizer.config.SourceType
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.native.JsonMethods._


object JobMetadata {
  def apply(jobType: String, processType: String, sourceType: String, destinationType: String, sourceTableList: String, destinationTableList: String, generatedSparkConf: SparkConf): JobMetadata = {

    new JobMetadata(
      SparkJobType.withName(jobType),
      ProcessType.withName(processType),
      SourceType.withName(sourceType),
      DestinationType.withName(destinationType),
      sourceTableList,
      destinationTableList,
      generatedSparkConf)
  }
  def apply(appConfig: String): JobMetadata = {

    val parsedJson = parse(appConfig)
    implicit val formats = DefaultFormats


    val generatedSparkConf: SparkConf = new SparkConf(true)

    val sourceCreds: SparkConf = (parsedJson \ "sourcetype").extract[String] match {
      case "cassandra" => {
        val cassCreds = (parsedJson \ "sourcecredentials").extract[cassandraCredentials]
        generatedSparkConf.set("spark.cassandra.connection.host", cassCreds.hostname)
          .set("spark.cassandra.auth.username", cassCreds.username)
          .set("spark.cassandra.auth.password", cassCreds.password)
      }
      case "delta" => {
        val s3Creds = (parsedJson \ "sourcecredentials").extract[s3Credentials]
        generatedSparkConf.set("fs.s3a.connection.ssl.enabled", value = "false")
          .set("fs.s3a.endpoint", s3Creds.url)
          .set("fs.s3a.access.key", s3Creds.accessKey)
          .set("fs.s3a.secret.key", s3Creds.secretKey)
      }

      case "jdbc" => {
        val jdbcCreds = (parsedJson \ "sourcecredentials").extract[jdbcCredentials]
        generatedSparkConf.set("jdbc.username", jdbcCreds.username)
          .set("jdbc.password", jdbcCreds.password)
          .set("jdbc.host", jdbcCreds.hostname)
          .set("jdbc.port", jdbcCreds.port.toString)
          .set("jdbc.uri", jdbcCreds.uri)
      }
    }

    val targetCreds: SparkConf = (parsedJson \ "destinationtype").extract[String] match {
      case "cassandra" => {
        val cassCreds = (parsedJson \ "destinationcredentials").extract[cassandraCredentials]
        generatedSparkConf.set("spark.cassandra.connection.host", cassCreds.hostname)
          .set("spark.cassandra.auth.username", cassCreds.username)
          .set("spark.cassandra.auth.password", cassCreds.password)
      }
      case "delta" => {
        val s3Creds = (parsedJson \ "destinationcredentials").extract[s3Credentials]
        generatedSparkConf.set("fs.s3a.connection.ssl.enabled", value = "false")
          .set("fs.s3a.endpoint", s3Creds.url)
          .set("fs.s3a.access.key", s3Creds.accessKey)
          .set("fs.s3a.secret.key", s3Creds.secretKey)
      }
    }

    apply(
      (parsedJson \ "jobtype").extract[String],
      (parsedJson \ "processtype").extract[String],
      (parsedJson \ "sourcetype").extract[String],
      (parsedJson \ "destinationtype").extract[String],
      (parsedJson \ "sourcetable").extract[String],
      (parsedJson \ "destinationtable").extract[String],
      generatedSparkConf)

  }
}
case class JobMetadata(jobTypes: SparkJobType,
                       processType: ProcessType,
                       sourceType: SourceType,
                       destinationType: DestinationType,
                       sourceTable:String,
                       destinationTable:String,
                       sparkConf:SparkConf)

