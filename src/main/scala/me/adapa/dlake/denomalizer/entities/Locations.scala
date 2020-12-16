package me.adapa.dlake.denomalizer.entities

sealed trait locationClass {
  def getTableName:String ;
  def getGroupName:String ;
  def getSuffix:String;
  def getKey:String;
}

class S3Location(table_name : String,table_key:String,bucket_name: String,suffix_path:String) extends locationClass {
  override def getTableName: String = return table_name

  override def getGroupName: String = return bucket_name

  override def getSuffix: String = return suffix_path

  override def getKey: String = return table_key
}

class CassandraLocation(tableName : String,keyspaceName:String,idkey:String) extends locationClass {
  override def getTableName: String = return tableName

  override def getGroupName: String = return keyspaceName

  override def getSuffix: String = return null

  override def getKey: String = return  idkey
}
