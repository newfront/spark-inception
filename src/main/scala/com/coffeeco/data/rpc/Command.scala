package com.coffeeco.data.rpc

object Command {}

/**
 * The Remote Network Command
 * @param notebookId The Notebook Identifier associated with the command
 * @param paragraphId The Paragraph Identifier associated with the Notebook
 * @param command The command to evaluate (note: unsafe!)
 * @param requestId The RequestId that can be used for tracing
 * @param userId The Optional userId associated with the command (will runAs SPARK_USER=this)
 */

case class NetworkCommand(
  notebookId: String,
  paragraphId: String,
  command: String,
  requestId: String,
  userId: Option[String] = Some("nobody")) extends Serializable

/**
 * The Results of processing the NetworkCommand
 * @param requestId The requestId associated with the command + context
 * @param commandStatus The status of evaluating the command
 * @param consoleOutput The result (output) from the remote command
 */
case class NetworkCommandResult(
  requestId: String,
  commandStatus: String,
  consoleOutput: String) extends Serializable
