package me.adapa.dlake.denomalizer.entities.metadata

import me.adapa.dlake.denomalizer.config.SparkJobType.SparkJobType
import me.adapa.dlake.denomalizer.entities.locationClass
import org.apache.spark.SparkConf

case class DenormMetaData(sparkJobType: SparkJobType,
                          sourceLocationInfo: locationClass,
                          destLocationInfo: locationClass,
                          sparkConf: SparkConf) extends Basemetadata
