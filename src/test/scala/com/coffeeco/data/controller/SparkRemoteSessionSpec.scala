package com.coffeeco.data.controller

import com.coffeeco.data.{SharedSparkSql, SparkInceptionControllerApp}
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.coffeeco.data.TestHelper.fullPath
import com.coffeeco.data.rpc.NetworkCommand

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

  "SparkRemoteSession" should " process a NetworkCommand and return NetworkCommandResults" in {
    val simpleCommand = NetworkCommand(
      "notebook1",
      "paragraph1",
      command = "spark.sparkContext.sparkUser",
      "request1", Some("anyone"))

    val results = remoteSession.processCommand(simpleCommand)
    results.head.commandStatus shouldEqual "Success"
    val output = results.head.consoleOutput.split("^\\$\\w*\\:\\W[A-Za-z]*\\W\\=\\W*").last
    output shouldEqual "anyone"
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

  "SparkRemoteSession" should " process all commands, like scala repl :paste" in {
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
    val results = remoteSession.processCommand(
      NetworkCommand("notebook1", "paragraph3",
        command =
      """
        |spark.sql("show tables").show(10)
        |""".stripMargin,
      "request4", Some("anyone")))
    val h = ""
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    remoteSession.close()
    super.afterAll()
  }

}
