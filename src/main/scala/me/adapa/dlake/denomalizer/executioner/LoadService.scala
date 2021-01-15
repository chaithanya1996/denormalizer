package me.adapa.dlake.denomalizer.executioner

import io.delta.tables.DeltaTable
import me.adapa.dlake.denomalizer.config.TableMapperConfig.getPartitionColName
import me.adapa.dlake.denomalizer.config.{DestinationType, SparkJobType}
import me.adapa.dlake.denomalizer.entities.metadata.LoadMetaData
import me.adapa.dlake.denomalizer.executioner.LoadService.{readerService, writerService}
import me.adapa.dlake.denomalizer.executioner.PartitionServce.repartitionDfMonth
import me.adapa.dlake.denomalizer.executioner.SparkUtility.getIncomingPartitionKeys
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LoadService {

  def apply(jobMetadata: LoadMetaData): LoadService = new LoadService(jobMetadata)

  def readerService(jobMetadata: LoadMetaData, sparkSessionBuiltObject:SparkSession): DataFrame = {

    jobMetadata.sparkJobType match {
      case SparkJobType.Initial => {
       sparkSessionBuiltObject.read
          .format("json")
          .option("header", value = true)
          .load(s"s3a://${jobMetadata.sourceLocationInfo.getGroupName}/${jobMetadata.sourceLocationInfo.getSuffix}/${jobMetadata.sourceLocationInfo.getTableName}/${jobMetadata.fileName}");
      }


      case SparkJobType.Incremental => {
        sparkSessionBuiltObject.read
          .format("json")
          .option("header", value = true)
          .load(s"s3a://${jobMetadata.sourceLocationInfo.getGroupName}/${jobMetadata.sourceLocationInfo.getSuffix}/${jobMetadata.sourceLocationInfo.getTableName}/${jobMetadata.fileName}").cache();

      }
    }
  }

  def writerService(sparkDataFrameToWrite : DataFrame, jobMetadata: LoadMetaData, sparkSessionBuiltObject:SparkSession): Unit ={

    jobMetadata.sparkJobType match {
      case SparkJobType.Initial =>
        jobMetadata.destinationType match {
          case DestinationType.cassandra => {

            // TODO Implement A source to Destination Colum Mapper for load and implementation for denorm
            val sparkDataFrameToWriteRenamed = SparkUtility.ConvertDataframeColToLowerCase(sparkDataFrameToWrite)

            sparkDataFrameToWriteRenamed.write
              .cassandraFormat
              .option("keyspace", jobMetadata.destLocationInfo.getGroupName)
              .option("table", jobMetadata.destLocationInfo.getTableName)
              .mode(SaveMode.Append)
              .save();
          }

          case DestinationType.delta => {
            sparkDataFrameToWrite.write
              .format("delta")
              .partitionBy("_partition_key")
              .mode(SaveMode.Overwrite)
              .save(s"s3a://${jobMetadata.destLocationInfo.getGroupName}/${jobMetadata.destLocationInfo.getSuffix}/${jobMetadata.destLocationInfo.getTableName}");

          }
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
              .option("keyspace", jobMetadata.destLocationInfo.getGroupName)
              .option("table", jobMetadata.destLocationInfo.getTableName)
              .mode(SaveMode.Append)
              .save();
          }

          case DestinationType.delta => {
            val currentDeltaTable = DeltaTable.forPath(sparkSessionBuiltObject, s"s3a://${jobMetadata.destLocationInfo.getGroupName}/${jobMetadata.destLocationInfo.getSuffix}/${jobMetadata.destLocationInfo.getTableName}")
            val deltaTablePartitionKey = getIncomingPartitionKeys(sparkDataFrameToWrite,"_partition_key")

            deltaTablePartitionKey.foreach(parKey => currentDeltaTable.as("currentDelta")
              .merge(sparkDataFrameToWrite.filter(col("_partition_key") === parKey ).as("sparkDf"), s"currentDelta._partition_key = ${parKey} and sparkDf.${jobMetadata.sourceLocationInfo.getKey} = currentDelta.${jobMetadata.destLocationInfo.getKey}")
              .whenMatched
              .updateAll
              .whenNotMatched
              .insertAll
              .execute())
          }
        }
      }
    }
  }

}

class LoadService(jobMetadata: LoadMetaData)  {

  val sparkSessionBuiltObject: SparkSession = SparkSession.builder.config(jobMetadata.sparkConf)
    .master("local[*]")
    .appName("Denormalizer Application")
    .getOrCreate()


  def execute(): Unit = {

    //TODO Partition Support for Spark Data

    val sourceRawDf: DataFrame = readerService(jobMetadata,sparkSessionBuiltObject)
    val sourceRepartionsdDf: DataFrame = repartitionDfMonth(sourceRawDf,getPartitionColName(jobMetadata.sourceLocationInfo.getTableName))
    sourceRepartionsdDf.printSchema()
    writerService(sourceRepartionsdDf,jobMetadata,sparkSessionBuiltObject)

    sparkSessionBuiltObject.stop()
  }
}
