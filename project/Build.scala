import sbt._
import Keys._

object SilexBuild extends Build {
  val mysettings = Defaults.defaultSettings ++ Seq(
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/root-doc.txt")
  )

  lazy val silex = Project(
    id = "silex",
    base = file("./src"),
    settings = mysettings
  )
}
