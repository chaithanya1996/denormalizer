package me.adapa.dlake.denomalizer.entities

case class JoinPathInfo(baseTable:String,lookupTable:List[String],joinColumnsList:List[String])
