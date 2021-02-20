package me.adapa.dlake.denomalizer.entities

case class TableConfig(baseTable:String, lookupTable:List[String],joinColumnsList:List[String], selectColList:List[List[String]])