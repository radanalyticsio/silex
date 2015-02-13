name := "silex"

organization := "com.redhat.et"

version := "0.0.1"

val SPARK_VERSION = "1.2.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % SPARK_VERSION

libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION

seq(bintrayPublishSettings:_*)

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/root-doc.txt")
