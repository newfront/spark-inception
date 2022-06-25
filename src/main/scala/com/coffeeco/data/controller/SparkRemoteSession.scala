package com.coffeeco.data.controller

import com.coffeeco.data.SparkInceptionControllerApp
import com.coffeeco.data.controller.SparkRemoteSession.InitializationCommands
import com.coffeeco.data.rpc.{NetworkCommand, NetworkCommandResult}
import com.coffeeco.data.traits.SparkApplication
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.log4j.Logger
import org.apache.spark.repl.SparkILoop

import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths}
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader
import scala.tools.nsc.Settings
import scala.util.Properties.{javaVersion, javaVmName, versionString}

object SparkRemoteSession {
  val logger: Logger = Logger.getLogger("com.coffeeco.data.controller.SparkRemoteSession")

  /*
    The initialization command bootstraps the internal SparkILoop.
    See https://github.com/apache/spark/blob/v3.2.1/repl/src/main/scala-2.12/org/apache/spark/repl/SparkILoop.scala#L45
    for reference.
    - These commands are evaluated in order, creating and customizing your Spark Session.
    - If there are specific things you need to Ensure exist - then this technique helps you setup exactly that
    */
  val InitializationCommands: Seq[String] = Seq(
    "import com.coffeeco.data.SparkInceptionControllerApp",
    """
    println("SPARK INCEPTION REPL: Initialization")
    """,
    """
    @transient val spark = SparkInceptionControllerApp.sparkSession
    println(spark)
    """,

    "import org.apache.spark.SparkContext._",
    "import spark.implicits._",
    "import org.apache.spark.sql.functions._"
  )

  @transient @volatile protected[data] var sparkRemoteSession: SparkRemoteSession[_] = _

  def apply(replInitCommands: Seq[String] = InitializationCommands): SparkRemoteSession[_] = {
    if (sparkRemoteSession == null) {
      synchronized {
        if (sparkRemoteSession == null) {
          sparkRemoteSession = new SparkRemoteSession(
            app = SparkInceptionControllerApp,
            replInitializationCommands = replInitCommands)
        }
      }
    }
    sparkRemoteSession
  }

}

class SparkRemoteSession[T <: SparkApplication : ClassTag](app: T, replInitializationCommands: Seq[String]) {
  import scala.tools.nsc.interpreter.JPrintWriter
  import SparkRemoteSession.logger
  // application enables you to get to the SparkSession, SparkConf, etc
  // we use this to ensure we create a shared context since the SparkRemoteSession
  // exists (inside of) the SparkInceptionControllerApp, and runs in its own separate
  // runtime Context.

  val replClassDirectory: String = app.sparkSession.conf.get(
    app.appConfigProps.ReplClassDir,
    sys.props.getOrElse("java.io.tmpdir", ""))

  // enables setting of user_jars, if empty nothing will be loaded
  val extraJarsDir: String = app.sparkSession.conf.get(app.appConfigProps.ReplExtraJarsDir, "")

  val ReplOutputStream = new ByteArrayOutputStream()
  private[this] final val outputStream = new JPrintWriter(ReplOutputStream, true)
  private[this] var _contextClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader

  /* where our dynamic classes will be written out to */
  val outputDir: File = {
    val f = Files.createTempDirectory(Paths.get(replClassDirectory), "spark").toFile
    f.deleteOnExit()
    f
  }

  private def updateClassLoader(classLoader: ClassLoader): Unit = {
    _contextClassLoader = classLoader
  }

  /**
   * Takes the resulting output from the Repl Output Buffer, cleans the buffer, and returns the output
   * @return
   */
  def readOutput(): Seq[String] = {
    synchronized {
      val result = this.ReplOutputStream.toString("utf-8").split("\n").toSeq
      this.ReplOutputStream.reset()
      result
    }
  }

  def close(): Unit = {
    this.ReplOutputStream.flush()
    // need to return this result back to the Spark Application when closing up...
    val finalOutput = readOutput()
    // clean up after ourselves
    this.ReplOutputStream.close()
  }

  logger.info(s"replClassDirectory=$replClassDirectory fullPath=${outputDir.getAbsolutePath}")

  lazy val sparkILoop: SparkILoop = {
    val sets: Settings = new Settings
    sets.processArguments(
      List("-Yrepl-class-based", "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"),
      processAll = true
    )
    if (sets.classpath.isDefault) {
      logger.info(s"sets.classpath=default update:java.class.path")
      sets.classpath.value = sys.props("java.class.path")
    }
    sets.usejavacp.value = true

    // adding additional user jars
    if (extraJarsDir.nonEmpty) {
      logger.info(s"spark.repl.extra.jars.dir=$extraJarsDir")
      val jarsDir = new File(extraJarsDir)
      val jars: Seq[URL] = jarsDir.listFiles().map { _.toURI.toURL }.toSeq
      val classPathValue = sets.classpath.value
      val updateClassPathValue = (jars ++ Seq(classPathValue)).mkString(File.pathSeparator)
      logger.info(s"updated.classpath.value: $updateClassPathValue\n")
      sets.classpath.value = updateClassPathValue
      updateClassLoader(new URLClassLoader(jars, Thread.currentThread().getContextClassLoader))
    }
    // set the reference classes that can be used for T variables and use in the local Spark ILoop
    sets.embeddedDefaults(_contextClassLoader)

    logger.info(s"generating the SparkILoop")

    lazy val sparkILoop = new SparkILoop(None, outputStream) {
      override val initializationCommands: Seq[String] = InitializationCommands
      /* replace the standard Spark welcome message */
      override def printWelcome(): Unit = {
        import org.apache.spark.SPARK_VERSION
        echo("""
   _____                  __      ____                      __  _                ______            __             ____
  / ___/____  ____ ______/ /__   /  _/___  ________  ____  / /_(_)___  ____     / ____/___  ____  / /__________  / / /__  _____
  \__ \/ __ \/ __ `/ ___/ //_/   / // __ \/ ___/ _ \/ __ \/ __/ / __ \/ __ \   / /   / __ \/ __ \/ __/ ___/ __ \/ / / _ \/ ___/
 ___/ / /_/ / /_/ / /  / ,<    _/ // / / / /__/  __/ /_/ / /_/ / /_/ / / / /  / /___/ /_/ / / / / /_/ /  / /_/ / / /  __/ /
/____/ .___/\__,_/_/  /_/|_|  /___/_/ /_/\___/\___/ .___/\__/_/\____/_/ /_/   \____/\____/_/ /_/\__/_/   \____/_/_/\___/_/   spark.version %s
    /_/                                          /_/
         """.format(SPARK_VERSION))
        val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
          versionString, javaVmName, javaVersion)
        echo(welcomeMsg)
        echo("Type in expressions to have them evaluated.")
        echo("Type :help for more information.")
      }
    }
    sparkILoop.settings = sets
    // load the jar for this class
    sparkILoop.createInterpreter()
    sparkILoop.initializeSpark()
    sparkILoop.initializeSynchronous()

    sparkILoop
  }
  // sparkILoop.replOutput (gives all the information from IMain)
  // sparkILoop.settings (cascade of all the things)

  /**
   * Will trigger an action (evaluating the Remote NetworkCommand in the SparkILoop via the SparkRemoteSession)
   * @param cmd The NetworkCommand being processed
   * @return The results of processing the command
   */
  def processCommand(cmd: NetworkCommand): Seq[NetworkCommandResult] = {
    // what does the inbound command look like?
    val user = cmd.userId.getOrElse("nobody")
    logger.info(s"network.command.received\n" +
      s"cmd.notebookId=${cmd.notebookId}\n " +
      s"cmd.paragraphId=${cmd.paragraphId}\n" +
      s"cmd.command=${cmd.command}\n" +
      s"cmd.requestId=${cmd.requestId}\n" +
      s"cmd.userId=$user"
    )

    // Regarding Security: The following check is a basic example of user gating:
    // ONLY IF the cmd.userId matches the SparkInceptionController (HADOOP_USER_NAME) do we proceed
    if (authCheck(user)) {
      logger.info(s"security.gate.passed")
      // evaluate the command
      val results = cmd.command.split("\n").filter(_.nonEmpty).toSeq.map { networkCommand =>
        val result = sparkILoop.interpret(networkCommand, synthetic = true)
        val consoleOutput = readOutput()
        (result, consoleOutput)
      }
      results.map { result =>
        NetworkCommandResult(
          requestId = cmd.requestId,
          commandStatus = result._1.toString,
          consoleOutput = result._2.mkString("\n")
        )
      }
    } else Seq(NetworkCommandResult(cmd.requestId, "Failure", s"$user is not authorized"))
  }

  def authCheck(user: String): Boolean = {
    user == app.sparkSession.sparkContext.sparkUser
  }

}
