package me.adapa.dlake.denomalizer.config

object DestinationType extends Enumeration {
  type DestinationType = Value
  val cassandra,delta= Value

  override def toString(): String = super.toString()
}
