package me.adapa.dlake.denomalizer.config

sealed trait MdcLoadExceptions
final case class UnknownParitionKeyError(private val message: String = "No Partition Key is Specified for Selected Table")  extends Exception(message) with MdcLoadExceptions
final case class BaseTableNotConfiguredInDB(private val message: String = "No join Path is found the DB for givenBaseTable")  extends Exception(message) with MdcLoadExceptions
final case class BadTableConfigException(private val message: String = "No join Path is found the DB for givenBaseTable") extends Exception(message) with MdcLoadExceptions