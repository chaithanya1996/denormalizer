package me.adapa.dlake.denomalizer.entities


sealed trait DenromCredentials
case class cassandraCredentials(username:String, password: String,hostname:String, port:Int = 9042) extends DenromCredentials
case class s3Credentials(url:String, accessKey:String, secretKey:String) extends DenromCredentials
case class jdbcCredentials(username:String, password: String,hostname:String, port:Int = 9042,uri:String) extends DenromCredentials