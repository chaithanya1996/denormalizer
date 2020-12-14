package me.adapa.dlake.denomalizer.entities.metadata

import me.adapa.dlake.denomalizer.config.{DestinationType, SourceType, SparkJobType}
import me.adapa.dlake.denomalizer.entities._
import org.apache.spark.SparkConf
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

object MetaDataFactory {

  def getLoadMetadata(appConfig: String): LoadMetaData = {

    val parsedJson = parse(appConfig)
    implicit val formats: DefaultFormats.type = DefaultFormats

      val generatedSparkConf: SparkConf = new SparkConf(true)
      val locationSource: locationClass = (parsedJson \ "source_type").extract[String] match {
        case "s3" => {

          val extractedS3Creds: s3Credentials = (parsedJson \ "source_credentials").extract[s3Credentials]

          generatedSparkConf.set("fs.s3a.connection.ssl.enabled", value = "false")
            .set("fs.s3a.endpoint", extractedS3Creds.url)
            .set("fs.s3a.access.key", extractedS3Creds.accessKey)
            .set("fs.s3a.secret.key", extractedS3Creds.secretKey)

          (parsedJson \ "load_metadata" \ "source_table").extract[S3Location]

        }
        case _ => throw new Exception("unknown source_type in json")
      }

      val locationDest: locationClass = (parsedJson \ "destination_type").extract[String] match {
        case "cassandra" => {
          val cassCreds = (parsedJson \ "source_credentials").extract[cassandraCredentials]
          generatedSparkConf.set("spark.cassandra.connection.host", cassCreds.hostname)
            .set("spark.cassandra.auth.username", cassCreds.username)
            .set("spark.cassandra.auth.password", cassCreds.password)

          (parsedJson \ "load_metadata" \ "destination_table").extract[CassandraLocation]
        }
        case "delta" => {
          val extractedS3Creds: s3Credentials = (parsedJson \ "source_credentials").extract[s3Credentials]
          generatedSparkConf.set("fs.s3a.connection.ssl.enabled", value = "false")
            .set("fs.s3a.endpoint", extractedS3Creds.url)
            .set("fs.s3a.access.key", extractedS3Creds.accessKey)
            .set("fs.s3a.secret.key", extractedS3Creds.secretKey)

          (parsedJson \ "load_metadata" \ "destination_table").extract[S3Location]
        }
      }

      val jobType = (parsedJson \ "load_metadata" \ "job_type").extract[String]
      val destinationType = (parsedJson \ "destination_type" ).extract[String]
      val fileName = (parsedJson \ "load_metadata" \ "file_name").extract[String]
      LoadMetaData(SparkJobType.withName(jobType), fileName, DestinationType.withName(destinationType),locationSource, locationDest, generatedSparkConf)

  }

  def getDenormMetadata(appConfig: String) : DenormMetaData = {
    val parsedJson = parse(appConfig)
    implicit val formats: DefaultFormats.type = DefaultFormats

      val generatedSparkConf: SparkConf = new SparkConf(true)
      val locationSource: locationClass = (parsedJson \ "source_type").extract[String] match {

        case "cassandra" => {
          val cassCreds = (parsedJson \ "source_credentials").extract[cassandraCredentials]
          generatedSparkConf.set("spark.cassandra.connection.host", cassCreds.hostname)
            .set("spark.cassandra.auth.username", cassCreds.username)
            .set("spark.cassandra.auth.password", cassCreds.password)

          (parsedJson \ "load_metadata" \ "source_table").extract[CassandraLocation]
        }
        case "delta" => {
          val extractedS3Creds: s3Credentials = (parsedJson \ "source_credentials").extract[s3Credentials]
          generatedSparkConf.set("fs.s3a.connection.ssl.enabled", value = "false")
            .set("fs.s3a.endpoint", extractedS3Creds.url)
            .set("fs.s3a.access.key", extractedS3Creds.accessKey)
            .set("fs.s3a.secret.key", extractedS3Creds.secretKey)

          (parsedJson \ "load_metadata" \ "source_table").extract[S3Location]
        }
      }

      val locationDest: locationClass = (parsedJson \ "destination_type").extract[String] match {
        case "cassandra" => {
          val cassCreds = (parsedJson \ "destination_credentials").extract[cassandraCredentials]
          generatedSparkConf.set("spark.cassandra.connection.host", cassCreds.hostname)
            .set("spark.cassandra.auth.username", cassCreds.username)
            .set("spark.cassandra.auth.password", cassCreds.password)

          (parsedJson \ "denorm_metadata" \ "destination_table").extract[CassandraLocation]
        }
        case "delta" => {
          val extractedS3Creds: s3Credentials = (parsedJson \ "destination_credentials").extract[s3Credentials]
          generatedSparkConf.set("fs.s3a.connection.ssl.enabled", value = "false")
            .set("fs.s3a.endpoint", extractedS3Creds.url)
            .set("fs.s3a.access.key", extractedS3Creds.accessKey)
            .set("fs.s3a.secret.key", extractedS3Creds.secretKey)

          (parsedJson \ "denorm_metadata" \ "destination_table").extract[S3Location]
        }
      }

      val jobType = SparkJobType.withName((parsedJson \ "denorm_metadata" \ "job_type").extract[String])
      val sourceType = SourceType.withName((parsedJson \ "denorm_metadata" \ "job_type").extract[String])
      val destinationType = DestinationType.withName((parsedJson \ "denorm_metadata" \ "job_type").extract[String])
      DenormMetaData(jobType,sourceType,destinationType, locationSource, locationDest, generatedSparkConf)
  }
}
