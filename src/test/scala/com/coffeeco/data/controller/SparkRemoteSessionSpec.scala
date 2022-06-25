package com.coffeeco.data.controller

import com.coffeeco.data.{SharedSparkSql, SparkInceptionControllerApp}
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.coffeeco.data.TestHelper.fullPath
import com.coffeeco.data.rpc.{Command, NetworkCommand, Status}

class SparkRemoteSessionSpec extends AnyFlatSpec with Matchers with SharedSparkSql {

  override def conf: SparkConf = {
    val sparkWarehouseDir = fullPath("src/test/resources/spark-warehouse")
    val testConfigPath = fullPath("src/test/resources/application-test.conf")
    sys.props += (("config.file", testConfigPath))
    sys.props += (("HADOOP_USER_NAME", "anyone"))

    SparkInceptionControllerApp.sparkConf
      .setMaster("local[*]")
      .set("spark.app.id", appID)
      .set("spark.driver.cores", "2")
      .set("spark.sql.warehouse.dir", sparkWarehouseDir)
  }

  lazy val remoteSession: SparkRemoteSession[_] = SparkRemoteSession()

  "SparkRemoteSession" should " process a NetworkCommand, clean the output, and return NetworkCommandResults" in {
    val simpleCommand = NetworkCommand(
      "notebook1",
      "paragraph1",
      command = "spark.sparkContext.sparkUser",
      "request1", Some("anyone"))

    val results = remoteSession.processCommand(simpleCommand)
    results.head.commandStatus shouldEqual "Success"
    // testing that the spark.sparkContext.sparkUser command returned 'anyone'
    // without the leading $ires{num}: String = anyone
    // since we want to have input/output formatted for people
    // uses the remoteSession.filterOutput to do so
    results.head.consoleOutput shouldEqual "anyone"
  }

  "SparkRemoteSession" should " fail with bad Spark User" in {
    // gut check:
    val simpleCommandBlocked = NetworkCommand(
      "notebook1",
      "paragraph1",
      command = "spark.sql(\"show tables\").show(10)",
      "request2", None)

    val failedCommand = remoteSession.processCommand(simpleCommandBlocked)
    failedCommand.head.commandStatus shouldEqual "Failure"
  }

  "SparkRemoteSession" should " process all commands, not as nicely as scala repl :paste but similar idea" in {
    // gut check:
    val multilineCommand = NetworkCommand(
      "notebook1",
      "paragraph2",
      command =
        """
          |case class Person(name: String, age: Int)
          |val people = Seq(Person("scott",37),Person("willow",12),Person("clover",6))
          |val df = spark.createDataFrame(people)
          |df.createOrReplaceTempView("people")
          |""".stripMargin,
      "request3", Some("anyone"))

    val results = remoteSession.processCommand(multilineCommand)
    results.foreach { result => result.commandStatus shouldEqual "Success" }
  }

  "SparkRemoteSession" should " retain dynamic classes and table references" in {
    // note: this test depends on running the tests in series. This could be fixed but this
    // is good enough for now
    val results = remoteSession.processCommand(
      NetworkCommand("notebook1", "paragraph3",
        command =
      """
        |spark.sql("show tables").show(10)
        |""".stripMargin,
      "request4", Some("anyone")))
    results.head.commandStatus shouldEqual "Success"
    val outputTable = results.head.consoleOutput.split("\n")
    // note: if you want to do something useful with the output
    // rather than just viewing it (like the table in this example)
    // then it is much 'cheaper' to convert the DataFrame or Dataset
    // into CSV or JSON and pass the data back that way
    // otherwise you will hate yourself for doing what you'll see next
    outputTable(1) shouldEqual "|namespace|tableName|isTemporary|"
    outputTable(3) shouldEqual "|         |   people|       true|"
  }

  "SparkRemoteSession" should " handle %spark as the first line of a command, or assume spark scala as a default" in {
    // Ensure we can assume the command type - otherwise:
    // <console>:23: error: not found: value %
    //%spark
    //^
    val cmd = NetworkCommand("notebook1", "paragraph4",
      command =
        """
          |%spark
          |spark.sql("show databases").show(10)
          |""".trim.stripMargin,
      "request5", Some("anyone"))

    val parsed = cmd.parse()
    // notice that the output here is the result of evaluating
    // the spark.sql("show tables").show(10) via scala
    // the resulting table is formatted for the console
    parsed._1 shouldEqual Command.SparkCommand
    parsed._2.head shouldEqual "spark.sql(\"show databases\").show(10)"

    val results = remoteSession.processCommand(cmd)
    val outputTable = results.head.consoleOutput.split("\n")
    outputTable(1) shouldEqual "|namespace|"
    outputTable(3) shouldEqual "|  default|"
  }

  "SparkRemoteSession" should " fail fast, and return stacktrace when interpreting incorrect SparkSQL" in {
    // note: this test relies on all the tests running as a series
    // the Person case class, dataframe and temporary table must
    // exist for this test to pass.

    val cmd = NetworkCommand("notebook1", "paragraph5",
      command =
        """
          |%sql
          |spark.sql("select * from people").limit(10).show(10)
          |""".trim.stripMargin,
      "request6", Some("anyone"))

    val parsed = cmd.parse()
    // notice that the output here is the result of evaluating
    // the spark.sql("show tables").show(10) via sql
    // the resulting table is newline separated json
    // cause that was easiest
    parsed._1 shouldEqual Command.SparkSQLCommand
    parsed._2.head shouldEqual "spark.sql(\"select * from people\").limit(10).show(10)"
    val results = remoteSession.processCommand(cmd)
    results.head.commandStatus shouldEqual Status.Failure
    results.head.consoleOutput.split("\n").head shouldEqual "org.apache.spark.sql.catalyst.parser.ParseException:"

  }

  "SparkRemoteSession" should "return the results of pure SparkSQL as newline separated json" in {
    // note: this test relies on all the tests running as a series
    // the Person case class, dataframe and temporary table must
    // exist for this test to pass.

    val cmd = NetworkCommand("notebook1", "paragraph6",
      command =
        """
          |%sql
          |select * from people limit 10
          |""".trim.stripMargin,
      "request7", Some("anyone"))

    val parsed = cmd.parse()
    // notice that the output here is the result of evaluating
    // the spark.sql("show tables").show(10) via sql
    // the resulting table is newline separated json
    // cause that was easiest
    parsed._1 shouldEqual Command.SparkSQLCommand
    parsed._2.head shouldEqual "select * from people limit 10"
    val results = remoteSession.processCommand(cmd)
    results.head.commandStatus shouldEqual Status.Success

    /*
    // the 3 dataframe rows we created by passing the series of commands (request2, paragraph2) in our
    // notebook1

    {"name":"scott","age":37}
    {"name":"willow","age":12}
    {"name":"clover","age":6}
    */
    val jsonTableRows = results.head.consoleOutput.split("\n")
    jsonTableRows(1) shouldEqual "{\"name\":\"scott\",\"age\":37}"
    jsonTableRows(2) shouldEqual "{\"name\":\"willow\",\"age\":12}"
    jsonTableRows(3) shouldEqual "{\"name\":\"clover\",\"age\":6}"
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    remoteSession.close()
    super.afterAll()
  }

}
