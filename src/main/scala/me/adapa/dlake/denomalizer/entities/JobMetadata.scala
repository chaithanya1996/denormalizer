package me.adapa.dlake.denomalizer.entities

import com.typesafe.config.Config
import me.adapa.dlake.denomalizer.config.DestinationType.DestinationType
import me.adapa.dlake.denomalizer.config.SparkJobType.SparkJobType
import me.adapa.dlake.denomalizer.config.ProcessType.ProcessType
import me.adapa.dlake.denomalizer.config.SourceType.SourceType
import me.adapa.dlake.denomalizer.config.SparkJobType
import me.adapa.dlake.denomalizer.config.DestinationType
import me.adapa.dlake.denomalizer.config.ProcessType
import me.adapa.dlake.denomalizer.config.SourceType

import scala.collection.JavaConverters._

object JobMetadata{

  def apply (jobType: String ,processType: String,sourceType: String,destinationType: String,sourceTableList:String,destinationTableList:String):JobMetadata = {
    new JobMetadata(
      SparkJobType.withName(jobType),
      ProcessType.withName(processType),
      SourceType.withName(sourceType),
      DestinationType.withName(destinationType),
      sourceTableList,
      destinationTableList
    )
  }

  def apply(appConfig:Config): JobMetadata = apply(
    appConfig.getString("jobtype"),
    appConfig.getString("processtype"),
    appConfig.getString("sourcetype"),
    appConfig.getString("destinationtype"),
    appConfig.getString("sourcetable"),
    appConfig.getString("destinationtable")
  )
}

case class JobMetadata(jobTypes: SparkJobType,
                       processType: ProcessType,
                       sourceType: SourceType,
                       destinationType: DestinationType,
                       sourceTable:String,
                       destinationTable:String)
