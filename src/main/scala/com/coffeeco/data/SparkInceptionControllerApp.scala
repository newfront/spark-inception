package com.coffeeco.data

import com.coffeeco.data.controller.SparkRemoteSession
import com.coffeeco.data.processor.NetworkCommandProcessor
import com.coffeeco.data.rpc.{NetworkCommand, NotebookExecutionDetails}
import com.coffeeco.data.traits.SparkStructuredStreamingApplication
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

object SparkInceptionControllerApp extends SparkStructuredStreamingApplication[NetworkCommand, NetworkCommand] {
  val logger: Logger = Logger.getLogger("com.coffeeco.data.SparkInceptionControllerApp")

  /**
   * Modify the base inputStream to
   */
  override lazy val inputStream: DataStreamReader = {
    super.streamReader()
      .schema(Encoders.product[NetworkCommand].schema)
  }

  override def outputStream(writer: DataStreamWriter[NetworkCommand])
                           (implicit sparkSession: SparkSession)
  : DataStreamWriter[NetworkCommand] = super.outputStream(writer)

  @transient implicit lazy val sparkRemoteSession: SparkRemoteSession[_] = SparkRemoteSession()
  /**
   * Your application will take a DataStreamReader, do something with the inbound micro-batch data
   * and then ultimately the data will flow back out of the application, through a DataStreamWriter
   *
   * @return The StreamingQuery (which is the full source->transform->sink.start)
   */
  override def runApp(): StreamingQuery = {
    import sparkSession.implicits._

    // note: the output table name is modifiable via configs - update the typesafe config to add a name
    val outputTableName: String = sparkSession.conf.get(appConfig.SinkToTableName, appConfig.SinkToTableNameDefault)
    // create a reference to our SparkRemoteSession instance (scoped at the driver)

    // the inception pipeline
    val processor = NetworkCommandProcessor(sparkSession)
    outputStream(
      processor.process(
        inputStream.load().as[NetworkCommand])
        .writeStream
    ).foreachBatch((ds: Dataset[NetworkCommand], batchId: Long) =>
      processBatch(ds, batchId)
    ).start()
      /*.foreachBatch((ds: Dataset[NetworkCommand], batchId: Long) => {
      val results = ds.map { networkCommand =>
        logger.info(s"executing.network.command=$networkCommand streaming.batch.id=$batchId")

        val res = remoteSessionController.processCommand(networkCommand)
        // wrap the execution details so we can write the results to redis
        NotebookExecutionDetails(
          networkCommand.notebookId,
          networkCommand.paragraphId,
          networkCommand.command,
          networkCommand.requestId,
          networkCommand.userId,
          res
        )
      }
      // note: you can use the batchId if you wanted to add [batchId|requestId] deduplication
      // will write the results out to a HashTable structure in Redis
      results.write
        .format("org.apache.spark.sql.redis")
        .option("table", s"$outputTableName")
        .mode(SaveMode.Append)
        .save()
    }).start()*/
  }

  def processBatch(ds: Dataset[NetworkCommand], batchId: Long): Unit = {
    import sparkSession.implicits._
    val outputTableName: String = sparkSession.conf.get(appConfig.SinkToTableName, appConfig.SinkToTableNameDefault)

    // Collect all of the Distributed Commands and bring down to the Driver
    // note: use .set("spark.app.source.options.stream.read.batch.size", "10") to reduce the number of commands you want to read
    val localResults = ds.collect().map { networkCommand =>
      //logger.info(s"executing.network.command=$networkCommand streaming.batch.id=$batchId")
      val res = sparkRemoteSession.processCommand(networkCommand)
      // wrap the execution details so we can write the results to redis
      NotebookExecutionDetails(
        networkCommand.notebookId,
        networkCommand.paragraphId,
        networkCommand.command,
        networkCommand.requestId,
        networkCommand.userId,
        res
      )
    }.toSeq

    val forRedis = sparkSession.createDataset[NotebookExecutionDetails](localResults)
    forRedis.write
      .format("org.apache.spark.sql.redis")
      .option("table", s"$outputTableName")
      .mode(SaveMode.Append)
      .save()
  }

  run()

}
