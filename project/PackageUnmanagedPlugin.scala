import sbt._
import Keys._

object packageUnmanaged extends Plugin {
  lazy val unmanagedPackageDir = SettingKey[java.io.File]("directory used by packageUnmanage") 
  lazy val packageUnmanaged = TaskKey[java.io.File]("packageUnmanaged", "Package jars to unmanaged jar directory")

  override val settings = Seq(
    unmanagedPackageDir := file(Path.userHome.absolutePath) / "sbtUnmanagedJars",
    packageUnmanaged <<= (packageBin in Compile, unmanagedPackageDir) map { (path, upath) =>
      {
        (s"mkdir -p $upath").!
        (s"cp -f $path $upath").!
        upath / path.getName
      }
    }
  )
}

