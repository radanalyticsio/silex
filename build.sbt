name := "silex"

organization := "com.redhat.et"

version := "0.0.7"

val SPARK_VERSION = "1.5.1"

scalaVersion := "2.10.4"

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % "provided",
    "joda-time" % "joda-time" % "2.7", 
    "org.joda" % "joda-convert" % "1.7",
    "org.scalatest" %% "scalatest" % "2.2.4" % Test,
    "org.json4s" %% "json4s-jackson" % "3.2.10" % "provided"
  )
)

seq(commonSettings:_*)

seq(bintraySettings:_*)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % SPARK_VERSION

libraryDependencies += "org.apache.spark" %% "spark-mllib" % SPARK_VERSION

seq(bintrayPublishSettings:_*)

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/root-doc.txt")

fork := true

site.settings

site.includeScaladoc()

site.jekyllSupport()

ghpages.settings

git.remoteRepo := "git@github.com:willb/silex.git"

lazy val silex = project in file(".")

lazy val spark = project.dependsOn(silex)
  .settings(commonSettings:_*)
  .settings(
    name := "spark",
    publishArtifact := false,
    publish := {},
    initialCommands in console := """
      |import org.apache.spark.SparkConf
      |import org.apache.spark.SparkContext
      |import org.apache.spark.SparkContext._
      |import org.apache.spark.rdd.RDD
      |val app = new com.redhat.et.silex.app.ConsoleApp()
      |val spark = app.context
      |com.redhat.et.silex.util.logging.consoleLogWarn
    """.stripMargin,
    cleanupCommands in console := "spark.stop")
