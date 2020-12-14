package me.adapa.dlake.denomalizer.executioner

import java.util.Properties
import me.adapa.dlake.denomalizer.config.SourceType.SourceType
import me.adapa.dlake.denomalizer.config.TableMapperConfig.getRelatedTables
import me.adapa.dlake.denomalizer.config.{DestinationType, SourceType}
import me.adapa.dlake.denomalizer.entities.locationClass
import me.adapa.dlake.denomalizer.entities.metadata.DenormMetaData
import me.adapa.dlake.denomalizer.executioner.DenormalizerService.readerService
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.annotation.tailrec

object DenormalizerService{

  def apply(jobMetadata: DenormMetaData): DenormalizerService = new DenormalizerService(jobMetadata)

  def ReadSingleTable(sourceType: SourceType,sourceLocationInfo: locationClass, sparkSessionBuiltObject:SparkSession) (sourceTable:String):DataFrame = {
    sourceType match {
      case SourceType.cassandra => sparkSessionBuiltObject.read
        .cassandraFormat
        .option("keyspace", sourceLocationInfo.getGroupName)
        .option("table", sourceLocationInfo.getTableName)
        .load();

      case SourceType.delta => sparkSessionBuiltObject.read
        .format("delta")
        .option("header", value = true)
        .load(s"s3a://${sourceLocationInfo.getGroupName}/${sourceLocationInfo.getSuffix}/${sourceLocationInfo.getTableName}");

//      case SourceType.jdbc => {
//        val jdbcConnectionProperties = new Properties()
//        jdbcConnectionProperties.setProperty("user", "sa")
//        jdbcConnectionProperties.setProperty("password", "A@adapa1996")
//
//        sparkSessionBuiltObject.read
//          .jdbc(s"jdbc:sqlserver://192.168.0.68:1433;databaseName=AssetAnswers_Demo",
//            sourceTable,
//            jdbcConnectionProperties)
//      }
    }
  }

  @tailrec
  def denormJoinTables(baseTable:DataFrame, lookupTable: List[DataFrame]): DataFrame = lookupTable match {
    case Nil => baseTable
    case headDf :: tailDfs => denormJoinTables(baseTable.join(headDf),tailDfs)
  }

  def readerService(jobMetadata: DenormMetaData, sparkSessionBuiltObject:SparkSession): DataFrame = {

    val relatedTablesList = getRelatedTables(jobMetadata.sourceLocationInfo.getTableName);
    def singleTableReaderTemplate = ReadSingleTable(jobMetadata.sourceType,jobMetadata.sourceLocationInfo,sparkSessionBuiltObject)(_);
    val lookupDFList = relatedTablesList.map( tName => singleTableReaderTemplate(tName))
    val sourceBaseTableDF = singleTableReaderTemplate(jobMetadata.sourceLocationInfo.getTableName)
    val joinedTables = denormJoinTables(sourceBaseTableDF,lookupDFList);

    return joinedTables;
  }

  def writerService(sparkDataFrameToWrite : DataFrame, jobMetadata: DenormMetaData): Unit = {

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
      case DestinationType.delta =>
        sparkDataFrameToWrite.write
          .format("delta")
          .mode(SaveMode.Append)
          .save(s"s3a://${jobMetadata.destLocationInfo.getGroupName}/${jobMetadata.destLocationInfo.getSuffix}/${jobMetadata.destLocationInfo.getTableName}");
    }
  }
}

class DenormalizerService(jobMetadata: DenormMetaData)  {

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
    // writerService(sourceRawDf,jobMetadata)

    sparkSessionBuiltObject.stop()
  }
}



