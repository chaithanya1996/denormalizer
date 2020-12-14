package me.adapa.dlake.denomalizer.entities.metadata

import me.adapa.dlake.denomalizer.config.DestinationType.DestinationType
import me.adapa.dlake.denomalizer.config.SparkJobType.SparkJobType
import me.adapa.dlake.denomalizer.entities.locationClass
import org.apache.spark.SparkConf

case class LoadMetaData(sparkJobType: SparkJobType,
                        fileName: String,
                        destinationType: DestinationType,
                        sourceLocationInfo: locationClass,
                        destLocationInfo: locationClass,
                        sparkConf: SparkConf)
