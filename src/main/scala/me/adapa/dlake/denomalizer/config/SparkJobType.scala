package me.adapa.dlake.denomalizer.config

object SparkJobType extends Enumeration {
  type SparkJobType = Value
  val Full,Incremental= Value
}