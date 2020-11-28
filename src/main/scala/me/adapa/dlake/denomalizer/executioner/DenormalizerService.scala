package me.adapa.dlake.denomalizer.executioner

import java.util.Properties

import me.adapa.dlake.denomalizer.entities.JobMetadata
import me.adapa.dlake.denomalizer.config.{DestinationType, SourceType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DenormalizerService{
  def apply(jobMetadata: JobMetadata, sparkConfig: SparkConf): DenormalizerService = new DenormalizerService(jobMetadata, sparkConfig)
}

class DenormalizerService(jobMetadata: JobMetadata , sparkConfig: SparkConf) extends ExecutionerService {

  val spark: SparkSession = SparkSession.builder.config(sparkConfig)
    .master("local[*]")
    .appName("Denormalizer Application")
    .getOrCreate()


  def readerService: DataFrame = {

    jobMetadata.sourceType match {

      case  SourceType.cassandra => spark.read
        .cassandraFormat
        .option("keyspace","aa_replica")
        .option("table",jobMetadata.sourceTable)
        .load();

      case  SourceType.delta => spark.read
        .format("delta")
        .option("header",value = true)
        .load(s"s3a://obj/AssetAnswers/${jobMetadata.sourceTable}");

      case  SourceType.jdbc => {

        val jdbcConnectionProperties  = new Properties()
        jdbcConnectionProperties.setProperty("user","sa")
        jdbcConnectionProperties.setProperty("password","A@adapa1996")

        spark.read
          .jdbc(s"jdbc:sqlserver://localhost:1433;databaseName=AssetAnswers_Demo",
            jobMetadata.sourceTable,
            jdbcConnectionProperties)
      }

    }
  }

  def writerService(sparkDataFrameToWrite : DataFrame): Unit ={

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

  def execute(): Unit ={

    // Read

    /* TODO
    *  Partition Support for Spark Data
    * */

    val sourceRawDf: DataFrame = readerService

    sourceRawDf.printSchema()

    writerService(sourceRawDf)

    // Transform

    //  Write

    spark.stop()
  }
}
