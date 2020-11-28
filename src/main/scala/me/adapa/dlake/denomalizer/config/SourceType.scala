package me.adapa.dlake.denomalizer.config

object SourceType extends Enumeration {
  type SourceType = Value

  val cassandra,delta,jdbc= Value
}
