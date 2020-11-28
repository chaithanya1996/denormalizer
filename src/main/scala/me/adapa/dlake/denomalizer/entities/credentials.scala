package me.adapa.dlake.denomalizer.entities

sealed trait denormcredentials
case class cassandraCredentials() extends denormcredentials
case class s3Credentials() extends denormcredentials
case class jdbcCredentials() extends denormcredentials
case class cassandraFileCredentials() extends denormcredentials