package com.coffeeco.data.controller

import com.coffeeco.data.SparkInceptionControllerApp
import com.coffeeco.data.controller.SparkRemoteSession.InitializationCommands
import com.coffeeco.data.rpc.Command.{SparkCommand, SparkSQLCommand, UnsupportedCommand}
import com.coffeeco.data.rpc.{Command, NetworkCommand, NetworkCommandResult, Status}
import com.coffeeco.data.traits.SparkApplication
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.log4j.Logger
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.DataFrame

import java.io.{File, PrintStream}
import java.net.URL
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader
import scala.tools.nsc.Settings
import scala.util.Properties.{javaVersion, javaVmName, versionString}
import scala.util.{Failure, Success, Try}

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

  val isInitialized: AtomicBoolean = new AtomicBoolean(false)

  // enables setting of user_jars, if empty nothing will be loaded
  val extraJarsDir: String = app.sparkSession.conf.get(app.appConfig.ReplExtraJarsDir, "")
  // storage location for dynamic compiled classes and for replaying the console history
  val replClassDirectory: String = app.sparkSession.conf.get(
    app.appConfig.ReplClassDir,
    sys.props.getOrElse("java.io.tmpdir", ""))

  // save the initial Console output stream
  private[this] final val initialConsoleOutputStream: PrintStream = System.out;

  // use this stream to capture console output (like when printing tables)
  val replOutputStream = new ByteArrayOutputStream()
  // forwarding std.out PrintStream to the replOutputStream (^^)
  private[this] final val consolePrintStream = new PrintStream(replOutputStream,true)
  private[this] final val outputStream = new JPrintWriter(replOutputStream, true)
  private[this] var _contextClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader

  /* where our dynamic classes will be written out to */
  val outputDir: File = {
    val f = Files.createTempDirectory(Paths.get(replClassDirectory), "spark").toFile
    f.deleteOnExit() // works with normal exit
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
      this.replOutputStream.flush()
      val result = this.replOutputStream.toString("utf-8")
        .split("\n")
        .toSeq
        .map(filterOutput)
        .filter(_.nonEmpty)
      this.replOutputStream.reset()
      result
    }
  }

  def initialize(): Unit = {
    if (!this.isInitialized.get()) {
      logger.info("sparkILoop.starting.up")
      if (sparkILoop.isInitializeComplete) {
        this.isInitialized.set(true)
      }
    }
  }

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

  /**
   * Cleans up the application. Should be part of the SparkApplication shutdown process
   *  additionally you can also just use sys.addShutdownHook { sparkRemoteSession.close() }
   *  just make sure you close up shop after stopping the outer Spark Application
   */
  def close(): Unit = {

    // these are all autoflushing - lets force flush then close up shop
    this.replOutputStream.flush()
    this.consolePrintStream.flush()
    this.outputStream.flush()
    // clean up after ourselves
    this.replOutputStream.close()
    this.outputStream.close()
    this.consolePrintStream.close()
    if (this.isInitialized.get()) {
      this.sparkILoop.closeInterpreter()
    }
  }
  // sparkILoop.replOutput (gives all the information from IMain)
  // sparkILoop.settings (cascade of all the things)

  /**
   * THIS IS WHERE THE MAGIC HAPPENS:
   * Will trigger an action (evaluating the Remote NetworkCommand in the SparkILoop via the SparkRemoteSession)
   * @param cmd The NetworkCommand being processed
   * @return The results of processing the command
   */
  def processCommand(cmd: NetworkCommand): Seq[NetworkCommandResult] = {
    initialize()
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
    val parsed = cmd.parse()
    if (parsed._1 != Command.UnsupportedCommand && authCheck(user)) {
      logger.debug(s"security.gate.passed")

      // evaluate the command
      val results = Console.withOut(consolePrintStream) {
        // redirect the console output
        System.setOut(Console.out)
        parsed._1 match {
          case SparkCommand =>
            logger.debug("spark.scala.commands")
            // trigger scala compiler/eval/loop directly at the SparkILoop
            // note: MAGIC HAPPENING HERE
            parsed._2.map(processSparkScala)
          case SparkSQLCommand =>
            logger.debug("spark.sql.commands")
            // we can natively submit the full SQL command
            // and match the output format
            parsed._2.map(processSparkSQL)
          case _ =>
            // we shouldn't get here...
            // make the compiler happy
            Seq((Status.Failure, Seq(s"${cmd.command} is not supported")))
        }
      }
      System.setOut(initialConsoleOutputStream)

      results.map { result =>
        NetworkCommandResult(
          requestId = cmd.requestId,
          commandStatus = result._1,
          consoleOutput = (result._2).mkString("\n")
        )
      }
    } else Seq(NetworkCommandResult(cmd.requestId, "Failure", s"$user is not authorized"))
  }

  /**
   * Using the SparkILoop, eval code, interact with live Spark directly
   * @param cmd The scala block to evaluate
   * @return The results of running the Scala block
   */
  def processSparkScala(cmd: String): (String, Seq[String]) = {
    val result = sparkILoop.interpret(cmd, synthetic = true)
    val consoleOutput = readOutput()
    (result.toString, consoleOutput)
  }

  /**
   * Using the Native app.sparkSession pointer run Spark SQL commands directly
   * @param cmd The SQL command will fail or succeed, stack trace will be output in the case of a failure
   * @return The results of interpreting the Spark SQL Command
   */
  def processSparkSQL(cmd: String): (String, Seq[String]) = {
    // note: In the case where you want delete protection for tables
    // or want to add specific limits (like limit 10 for open queries)
    // then you can parse the cmd string and add magic

    Try(app.sparkSession.sql(cmd)) match {
      case Success(df: DataFrame) =>
        (Status.Success, df.toJSON.collect().toSeq)
      case Failure(ex: Exception) =>
        ex.printStackTrace(consolePrintStream)
        (Status.Failure, readOutput())
      case Failure(thr: Throwable) =>
        thr.printStackTrace(consolePrintStream)
        (Status.Failure, readOutput())
      case _ =>
        (Status.Failure, Seq("Something went wrong"))
    }
  }

  /**
   * Basic check - not comprehensive security
   *
   * @param user The name of the user of an inbound Command
   * @return True if we want to process the inbound command, false for auth block
   */
  def authCheck(user: String): Boolean = {
    user == app.sparkSession.sparkContext.sparkUser
  }

  private[this] val scalaTypePrefixPattern = "^\\$ires\\w*\\:\\W[A-Za-z]*\\W\\=\\W*".r

  /**
   * Use this method to filter the Notebook processing output. Eg. do you care about class definitions or types?
   * what about formatting the output - like df.show(10, true) will convert to a console printed table.
   * Do you want to see that $iresN Unit: ()? probably not
   *
   * @param str The string to evaluate
   * @return empty string or cleaned string
   */
  def filterOutput(str: String): String = {

    val out = str match {
      case _ if str.startsWith("##") => ""
      case st if str.startsWith("$ires") =>
        scalaTypePrefixPattern.replaceFirstIn(st, "")
      case _ => str
    }
    out.trim
  }

}
