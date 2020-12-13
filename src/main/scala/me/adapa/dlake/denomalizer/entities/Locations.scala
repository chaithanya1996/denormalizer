package me.adapa.dlake.denomalizer.entities

sealed trait locationClass {
  def getTableName:String ;
  def groupname:String ;
  def getSuffix:String;
  def getKey:String;
}

class S3Location(tableName : String,bucketName: String,suffixpath:String) extends locationClass {
  override def getTableName: String = return tableName

  override def groupname: String = return bucketName

  override def getSuffix: String = return suffixpath

  override def getKey: String = return null
}

class CassandraLocation(tableName : String,keyspaceName:String,idkey:String) extends locationClass {
  override def getTableName: String = return tableName

  override def groupname: String = return keyspaceName

  override def getSuffix: String = return null

  override def getKey: String = return  idkey
}
