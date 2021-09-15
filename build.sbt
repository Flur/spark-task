import sbt.Keys.libraryDependencies

name := "spark-task"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)  
