import play.sbt.PlayRunHook
import sbt._

import scala.sys.process.Process

/**
  * Frontend build play run hook.
  * https://www.playframework.com/documentation/2.7.x/SBTCookbook
  */
object FrontendRunHook {
  def apply(base: File): PlayRunHook = {
    object UIBuildHook extends PlayRunHook {

      var process: Option[Process] = None

      /**
        * Change these commands if you want to use Yarn.
        */
      var yarnInstall: String = FrontendCommands.dependencyInstall
      var yarnRun: String = FrontendCommands.serve

      // Windows requires npm commands prefixed with cmd /c
      if (System.getProperty("os.name").toLowerCase().contains("win")){
        yarnInstall = "cmd /c" + yarnInstall
        yarnRun = "cmd /c" + yarnRun
      }

      /**
        * Executed before play run start.
        * Run npm install if node modules are not installed.
        */
      override def beforeStarted(): Unit = {
        if (!(base / "ui" / "node_modules").exists()) Process(yarnInstall, base / "ui").!
      }

      /**
        * Executed after play run start.
        * Run npm start
        */
      override def afterStarted(): Unit = {
        process = Option(
          Process(yarnRun, base / "ui").run
        )
      }

      /**
        * Executed after play run stop.
        * Cleanup frontend execution processes.
        */
      override def afterStopped(): Unit = {
        process.foreach(_.destroy())
        process = None
      }

    }

    UIBuildHook
  }
}