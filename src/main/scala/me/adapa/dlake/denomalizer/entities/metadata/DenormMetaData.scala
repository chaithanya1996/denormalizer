package me.adapa.dlake.denomalizer.entities.metadata

import me.adapa.dlake.denomalizer.config.DestinationType.DestinationType
import me.adapa.dlake.denomalizer.config.SourceType.SourceType
import me.adapa.dlake.denomalizer.config.SparkJobType.SparkJobType
import me.adapa.dlake.denomalizer.entities.locationClass
import org.apache.spark.SparkConf

case class DenormMetaData(sparkJobType: SparkJobType,
                          sourceType: SourceType,
                          destinationType: DestinationType,
                          sourceLocationInfo: locationClass,
                          destLocationInfo: locationClass,
                          sparkConf: SparkConf)
