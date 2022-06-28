package com.coffeeco.data

import com.coffeeco.data.controller.SparkRemoteSession
import com.coffeeco.data.processor.NetworkCommandProcessor
import com.coffeeco.data.rpc.{NetworkCommand, NotebookExecutionDetails}
import com.coffeeco.data.traits.SparkStructuredStreamingApplication
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery, Trigger}
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

  /**
   * Use to control the general options on the output Stream
   * @param writer The DataStreamWriter reference
   * @param sparkSession The implicit SparkSession
   * @return The decorated DataStreamWriter
   */
  override def outputStream(writer: DataStreamWriter[NetworkCommand])
                           (implicit sparkSession: SparkSession)
  : DataStreamWriter[NetworkCommand] = super.outputStream(writer)
    .trigger(Trigger.ProcessingTime("5 seconds"))

  @transient implicit lazy val sparkRemoteSession: SparkRemoteSession[_] = SparkRemoteSession()
  /**
   * Your application will take a DataStreamReader, do something with the inbound micro-batch data
   * and then ultimately the data will flow back out of the application, through a DataStreamWriter
   *
   * @return The StreamingQuery (which is the full source->transform->sink.start)
   */
  override def runApp(): StreamingQuery = {
    logger.info(s"run.app.called")
    import sparkSession.implicits._

    // the inception pipeline
    outputStream(
      NetworkCommandProcessor(sparkSession).process(
        inputStream.load().as[NetworkCommand]
      ).writeStream
    ).foreachBatch((ds: Dataset[NetworkCommand], batchId: Long) => processBatch(ds, batchId)
    ).start()
  }

  /**
   * For each micro-batch, collect the RPC command stream to the driver, process, and pass the results onto Redis
   * @param ds The RPC commands (via Redis)
   * @param batchId The batchId (can be used to skip reprocessing events if checkpoints are enabled)
   */
  def processBatch(ds: Dataset[NetworkCommand], batchId: Long): Unit = {
    import sparkSession.implicits._

    // Collect all of the Distributed Commands and bring down to the Driver
    val localResults = ds.collect().map { networkCommand =>
      // this is running on the driver now (not the executors)
      val res = sparkRemoteSession.processCommand(networkCommand)
      // wrap the execution details so we can write the results to redis
      NotebookExecutionDetails(
        networkCommand.notebookId,
        networkCommand.paragraphId,
        networkCommand.command,
        networkCommand.requestId,
        networkCommand.userId,
        res.commandStatus,
        res.consoleOutput
      )
    }.toSeq

    // generate a new dataframe and then write back to redis
    val forRedis = sparkSession.createDataset[NotebookExecutionDetails](localResults)
    forRedis
      .write
      .format("org.apache.spark.sql.redis")
      .options(sparkSession.sparkContext
        .getConf
        .getAllWithPrefix(appConfig.SinkStreamOptions).toMap[String, String])
      .mode(SaveMode.Append)
      .save()
  }

  run()

}
