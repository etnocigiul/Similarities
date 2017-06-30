name := "Sim Project"
 
version := "1.0"
 

scalaVersion := "2.11.8"

fork in run := true

javaOptions in run += "-Xms4g -Xmx10g -Xss16m"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
