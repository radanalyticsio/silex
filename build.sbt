name := "silex"

organization := "io.radanalytics"

version := "0.2.2"

val SPARK_VERSION = "2.2.2"

scalaVersion := "2.11.11"

crossScalaVersions := Seq("2.10.6", "2.11.11")

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % "provided",
    "joda-time" % "joda-time" % "2.7", 
    "org.joda" % "joda-convert" % "1.7",
    "org.apache.commons" % "commons-math3" % "3.6",
    "org.scalatest" %% "scalatest" % "2.2.4" % Test,
    "org.slf4j" % "slf4j-nop" % "1.7.6" % Test,
    "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
    "org.scalanlp" %% "breeze" % "0.13.1",
    "org.scalanlp" %% "breeze-natives" % "0.13.1"
  )
)

seq(commonSettings:_*)

seq(bintraySettings:_*)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % SPARK_VERSION

seq(bintrayPublishSettings:_*)

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/root-doc.txt")

(dependencyClasspath in Test) <<= (dependencyClasspath in Test).map(
  _.filterNot(_.data.name.contains("slf4j-log4j12"))
)

site.settings

site.includeScaladoc()

site.jekyllSupport()

ghpages.settings

git.remoteRepo := "git@github.com:radanalyticsio/silex.git"

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
      |val app = new io.radanalytics.silex.app.ConsoleApp()
      |val spark = app.context
    """.stripMargin,
    cleanupCommands in console := "spark.stop")
