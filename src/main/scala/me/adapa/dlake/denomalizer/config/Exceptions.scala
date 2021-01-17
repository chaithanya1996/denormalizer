package me.adapa.dlake.denomalizer.config

sealed trait MdcLoadExceptions
final case class UnknownParitionKeyError(private val message: String = "No Partition Key is Specified for Selected Table")  extends Exception(message) with MdcLoadExceptions
