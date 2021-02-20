package me.adapa.dlake.denomalizer.entities

import scala.collection.mutable

case class JoinPathInfo(baseTable:String,lookupTable:List[String],joinColumnsList:List[String],selectColList:List[String])
case class TableConfig(baseTable:String, lookupTable:List[String],joinColumnsList:List[String], selectColList:List[List[String]])
case class TableConfigRecord(baseTable:String, lookupTable:mutable.Map[String,String], selectColList:List[List[String]])