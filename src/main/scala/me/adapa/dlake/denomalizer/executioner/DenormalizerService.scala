package me.adapa.dlake.denomalizer.executioner

import com.typesafe.config.Config

import java.util.Properties
import me.adapa.dlake.denomalizer.config.SourceType.SourceType
import me.adapa.dlake.denomalizer.config.TableMapperConfig.getRelatedTables
import me.adapa.dlake.denomalizer.config.{DestinationType, SourceType, SparkJobType}
import me.adapa.dlake.denomalizer.entities.locationClass
import me.adapa.dlake.denomalizer.entities.metadata.DenormMetaData
import me.adapa.dlake.denomalizer.executioner.DenormalizerService.{readerService, writerService}
import me.adapa.dlake.denomalizer.util.DBUtil
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.annotation.tailrec

object DenormalizerService{

  def apply(jobMetadata: DenormMetaData,appAdminConfig:Config): DenormalizerService = new DenormalizerService(jobMetadata,appAdminConfig)

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
          .load(s"s3a://${sourceLocationInfo.getGroupName}/${sourceLocationInfo.getSuffix}/${sourceTable}");
    }
  }

  def ReadSingleTableCols(sourceType: SourceType,sourceLocationInfo: locationClass, sparkSessionBuiltObject:SparkSession) (sourceTable:String,colsToFilter:List[String]= List[String]()):DataFrame = {
    sourceType match {
      case SourceType.cassandra => {
        if(colsToFilter.isEmpty){
          sparkSessionBuiltObject.read
            .cassandraFormat
            .option("keyspace", sourceLocationInfo.getGroupName)
            .option("table", sourceLocationInfo.getTableName)
            .load()
        }else{
          sparkSessionBuiltObject.read
            .cassandraFormat
            .option("keyspace", sourceLocationInfo.getGroupName)
            .option("table", sourceLocationInfo.getTableName)
            .load().select(colsToFilter.map(col): _*)
        }
      }
      case SourceType.delta => {
        if(colsToFilter.isEmpty){
            sparkSessionBuiltObject.read
              .format("delta")
              .option("header", value = true)
              .load(s"s3a://${sourceLocationInfo.getGroupName}/${sourceLocationInfo.getSuffix}/${sourceTable}")
          }else{
            sparkSessionBuiltObject.read
              .format("delta")
              .option("header", value = true)
              .load(s"s3a://${sourceLocationInfo.getGroupName}/${sourceLocationInfo.getSuffix}/${sourceTable}")
              .select(colsToFilter.map(col): _*)}

      };
    }
  }

  @tailrec
  def denormJoinTables(baseTable:DataFrame, lookupTable: List[DataFrame] , joinOnCols:List[String]): DataFrame = lookupTable match {
    case Nil => baseTable
    case headDf :: tailDfs => {
      val currrentJoinCol :: tailJoinCol = joinOnCols;
      denormJoinTables(baseTable.join(headDf,usingColumn = currrentJoinCol),tailDfs,tailJoinCol)
    }
  }

  def readerService(jobMetadata: DenormMetaData, sparkSessionBuiltObject:SparkSession,appAdminConfig:Config): DataFrame = {

    val relatedJoinCOnfigFromDB = DBUtil.getAppJoinforTable(jobMetadata.sourceLocationInfo.getTableName.toLowerCase(),appAdminConfig)
    def singleTableReaderWithFilterColumnTemplate = ReadSingleTableCols(jobMetadata.sourceType,jobMetadata.sourceLocationInfo,sparkSessionBuiltObject)(_,_);
    val zippedLookupTableProperties = relatedJoinCOnfigFromDB.lookupTable.zip(relatedJoinCOnfigFromDB.joinColumnsList)

    val lookupDFList:List[DataFrame] = zippedLookupTableProperties.map(tName => {
      val listofCols = tName._2.trim().split(",").toList
      singleTableReaderWithFilterColumnTemplate(tName._1,listofCols)
    })

    val sourceBaseTableDF = singleTableReaderWithFilterColumnTemplate(jobMetadata.sourceLocationInfo.getTableName,List[String]())
    val joinedTables = denormJoinTables(sourceBaseTableDF,lookupDFList,relatedJoinCOnfigFromDB.joinColumnsList);
    return joinedTables;
  }

  def writerService(sparkDataFrameToWrite : DataFrame, jobMetadata: DenormMetaData): Unit = {

    jobMetadata.sparkJobType match {
      case SparkJobType.Initial  => {
        jobMetadata.destinationType match {

          case DestinationType.cassandra => {

            // TODO Implement A source to Destination Colum Mapper for load and implementation for denorm

            val sparkDataFrameToWriteRenamed = SparkUtility.ConvertDataframeColToLowerCase(sparkDataFrameToWrite)

            sparkDataFrameToWriteRenamed.write
              .cassandraFormat
              .option("keyspace", jobMetadata.destLocationInfo.getGroupName)
              .option("table", jobMetadata.destLocationInfo.getTableName)
              .mode(SaveMode.Overwrite)
              .save();
          }
          case DestinationType.delta =>
            sparkDataFrameToWrite.write
              .format("delta")
              .mode(SaveMode.Overwrite)
              .save(s"s3a://${jobMetadata.destLocationInfo.getGroupName}/${jobMetadata.destLocationInfo.getSuffix}/${jobMetadata.destLocationInfo.getTableName}");
        }

      }
      case SparkJobType.Incremental => {
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

  }
}

class DenormalizerService(jobMetadata: DenormMetaData,appAdminConfig:Config)  {

  val sparkSessionBuiltObject: SparkSession = SparkSession.builder.config(jobMetadata.sparkConf)
    .master("local[*]")
    .appName("Denormalizer Application")
    .getOrCreate()


  def execute(): Unit ={

    /* TODO
    *  Partition Support for Spark Data
    * */

    val sourceRawDf: DataFrame = readerService(jobMetadata,sparkSessionBuiltObject,appAdminConfig)
    sourceRawDf.printSchema()
     writerService(sourceRawDf,jobMetadata)

    sparkSessionBuiltObject.stop()
  }
}



