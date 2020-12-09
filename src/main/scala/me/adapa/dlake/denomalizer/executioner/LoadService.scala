package me.adapa.dlake.denomalizer.executioner

import io.delta.tables.DeltaTable

import me.adapa.dlake.denomalizer.entities.JobMetadata
import me.adapa.dlake.denomalizer.config.{DestinationType, SourceType, SparkJobType}
import me.adapa.dlake.denomalizer.executioner.LoadService.{readerService, writerService}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LoadService {

  def apply(jobMetadata: JobMetadata): LoadService = new LoadService(jobMetadata)

  def readerService(jobMetadata: JobMetadata, sparkSessionBuiltObject:SparkSession): DataFrame = {

    jobMetadata.jobTypes match {
      case SparkJobType.Initial => {
       sparkSessionBuiltObject.read
          .format("delta")
          .option("header", value = true)
          .load(s"s3a://obj/AssetAnswers/${jobMetadata.sourceTable}");
      }
      case SparkJobType.Incremental => {
        sparkSessionBuiltObject.read
          .format("delta")
          .option("header", value = true)
          .load(s"s3a://obj/AssetAnswers/${jobMetadata.sourceTable}");
      }
    }
  }

  def writerService(sparkDataFrameToWrite : DataFrame, jobMetadata: JobMetadata, sparkSessionBuiltObject:SparkSession): Unit ={

    jobMetadata.jobTypes match {
      case SparkJobType.Initial =>
        jobMetadata.destinationType match {
          case DestinationType.cassandra => {

            // TODO Implement A source to Destination Colum Mapper for load and implementation for denorm

            val ColumnNames = sparkDataFrameToWrite.columns
            val LowerCaseColumnNames = ColumnNames.map(x => x.toLowerCase)
            val columnNamesZipped = ColumnNames.zip(LowerCaseColumnNames).map(x => col(x._1).as(x._2))
            val sparkDataFrameToWriteRenamed = sparkDataFrameToWrite.select(columnNamesZipped: _*)

            sparkDataFrameToWriteRenamed.write
              .cassandraFormat
              .option("keyspace", "aa_replica")
              .option("table", jobMetadata.destinationTable)
              .mode(SaveMode.Append)
              .save();
          }

          case DestinationType.delta =>
            sparkDataFrameToWrite.write
              .format("delta")
              .mode(SaveMode.Overwrite)
              .save(s"s3a://obj/AssetAnswers/${jobMetadata.destinationTable}");

        }

      case SparkJobType.Incremental => {
        jobMetadata.destinationType match {
          case DestinationType.cassandra => {
            // TODO Implement A source to Destination Colum Mapper for load and implementation for denorm
            val ColumnNames = sparkDataFrameToWrite.columns
            val LowerCaseColumnNames = ColumnNames.map(x => x.toLowerCase)
            val columnNamesZipped = ColumnNames.zip(LowerCaseColumnNames).map(x => col(x._1).as(x._2))
            val sparkDataFrameToWriteRenamed = sparkDataFrameToWrite.select(columnNamesZipped: _*)

            sparkDataFrameToWriteRenamed.write
              .cassandraFormat
              .option("keyspace", "aa_replica")
              .option("table", jobMetadata.destinationTable)
              .mode(SaveMode.Append)
              .save();
          }

          case DestinationType.delta => {
            val currentDeltaTable = DeltaTable.forPath(sparkSessionBuiltObject, s"s3a://obj/AssetAnswers/${jobMetadata.destinationTable}")
            currentDeltaTable.as("currentDelta")
              .merge(sparkDataFrameToWrite.as("sparkDf"), sparkDataFrameToWrite.col(""))
              .whenMatched
              .updateAll
              .whenNotMatched
              .insertAll
              .execute()
          }
        }
      }
    }


  }

}

class LoadService(jobMetadata: JobMetadata) extends ExecutionerService {

  val sparkSessionBuiltObject: SparkSession = SparkSession.builder.config(jobMetadata.sparkConf)
    .master("local[*]")
    .appName("Denormalizer Application")
    .getOrCreate()


  def execute(): Unit ={

    /* TODO
    *  Partition Support for Spark Data
    * */

    val sourceRawDf: DataFrame = readerService(jobMetadata,sparkSessionBuiltObject)
    sourceRawDf.printSchema()
    writerService(sourceRawDf,jobMetadata,sparkSessionBuiltObject)

    sparkSessionBuiltObject.stop()
  }
}
