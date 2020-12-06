package me.adapa.dlake.denomalizer.executioner

import java.util.Properties

import me.adapa.dlake.denomalizer.entities.JobMetadata
import me.adapa.dlake.denomalizer.config.{DestinationType, SourceType}
import me.adapa.dlake.denomalizer.executioner.LoadService.{readerService, writerService}

import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LoadService{

  def apply(jobMetadata: JobMetadata, sparkConfig: SparkConf): LoadService = new LoadService(jobMetadata, sparkConfig)

  def readerService(jobMetadata: JobMetadata, sparkSessionBuiltObject:SparkSession): DataFrame = {

    jobMetadata.sourceType match {

      case  SourceType.cassandra => sparkSessionBuiltObject.read
        .cassandraFormat
        .option("keyspace","aa_replica")
        .option("table",jobMetadata.sourceTable)
        .load();

      case  SourceType.delta => sparkSessionBuiltObject.read
        .format("delta")
        .option("header",value = true)
        .load(s"s3a://obj/AssetAnswers/${jobMetadata.sourceTable}");

      case  SourceType.jdbc => {

        val jdbcConnectionProperties  = new Properties()
        jdbcConnectionProperties.setProperty("user","sa")
        jdbcConnectionProperties.setProperty("password","A@adapa1996")

        sparkSessionBuiltObject.read
          .jdbc(s"jdbc:sqlserver://192.168.0.89:1433;databaseName=AssetAnswers_Demo",
            jobMetadata.sourceTable,
            jdbcConnectionProperties)
      }

    }
  }

  def writerService(sparkDataFrameToWrite : DataFrame, jobMetadata: JobMetadata): Unit ={

    jobMetadata.destinationType match {

      case DestinationType.cassandra => {

        // TODO Implement A source to Destination Colum Mapper for load and implementation for denorm

        val ColumnNames = sparkDataFrameToWrite.columns
        val LowerCaseColumnNames = ColumnNames.map(x => x.toLowerCase)
        val columnNamesZipped = ColumnNames.zip(LowerCaseColumnNames).map(x => col(x._1).as(x._2))
        val sparkDataFrameToWriteRenamed = sparkDataFrameToWrite.select(columnNamesZipped:_*)

        sparkDataFrameToWriteRenamed.write
          .cassandraFormat
          .option("keyspace","aa_replica")
          .option("table",jobMetadata.destinationTable)
          .mode(SaveMode.Append)
          .save();
      }

      case DestinationType.delta =>
        sparkDataFrameToWrite.write
          .format("delta")
          .mode(SaveMode.Append)
          .save(s"s3a://obj/AssetAnswers/${jobMetadata.sourceTable}");

    }

  }

}

class LoadService(jobMetadata: JobMetadata, sparkConfig: SparkConf) extends ExecutionerService {

  val sparkSessionBuiltObject: SparkSession = SparkSession.builder.config(sparkConfig)
    .master("local[*]")
    .appName("Denormalizer Application")
    .getOrCreate()


  def execute(): Unit ={

    /* TODO
    *  Partition Support for Spark Data
    * */

    val sourceRawDf: DataFrame = readerService(jobMetadata,sparkSessionBuiltObject)
    sourceRawDf.printSchema()
    writerService(sourceRawDf,jobMetadata)

    sparkSessionBuiltObject.stop()
  }
}
