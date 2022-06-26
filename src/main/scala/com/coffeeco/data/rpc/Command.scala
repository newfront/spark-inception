package com.coffeeco.data.rpc

import com.coffeeco.data.rpc.Command.{Command, SparkCommand, SparkSQLCommand, UnsupportedCommand}

object Command extends Enumeration {
  type Command = Object
  val SparkCommand, SparkSQLCommand, UnsupportedCommand = Value
  def apply(str: String): Command = {
    // tests the header characters for each paragraph
    // do we have a known pattern? '%spark` or '%sql`
    if (str.trim.startsWith("%")) {
      str.trim match {
        case s if s.equals("%spark") => Command.SparkCommand
        case s if s.equals("%sql") => Command.SparkSQLCommand
        case _ => Command.UnsupportedCommand
      }
    } else Command.SparkCommand
  }
}

object Status {
  val Success: String = "Success"
  val Failure: String = "Failure"
}

trait NotebookRequest {
  val notebookId: String
  val paragraphId: String
  val command: String
  val requestId: String
  val userId: Option[String]
}
/**
 * The Remote Network Command
 * @param notebookId The Notebook Identifier associated with the command
 * @param paragraphId The Paragraph Identifier associated with the Notebook
 * @param command The command to evaluate (note: This is blind eval of unsafe code...)
 * @param requestId The RequestId that can be used for tracing
 * @param userId The Optional userId associated with the command (will runAs SPARK_USER=this)
 */
@SerialVersionUID(1L)
case class NetworkCommand(
  notebookId: String,
  paragraphId: String,
  command: String,
  requestId: String,
  userId: Option[String] = Some("nobody")) extends NotebookRequest with Serializable {
    @transient lazy val commands: Seq[String] = command.trim.split("\n").map(_.trim).filter(_.nonEmpty)

    def parse(): (Command.Value, Seq[String]) = {
      if (commands.nonEmpty) {
        val length = commands.length
        // check if the first line (header) exists
        val hasHeader = commands.head.startsWith("%")
        Command(commands.head) match {
          case UnsupportedCommand => (UnsupportedCommand, Seq.empty[String])
          case s: Command.Value =>
            (s, if (hasHeader) commands.splitAt(1)._2 else commands)
        }
      } else (Command.UnsupportedCommand, Seq.empty[String])
    }
}

trait NotebookResult {
  val requestId: String
  val commandStatus: String
  val consoleOutput: String
}

/**
 * The Results of processing the NetworkCommand
 * @param requestId The requestId associated with the command + context
 * @param commandStatus The status of evaluating the command
 * @param consoleOutput The result (output) from the remote command
 */
@SerialVersionUID(2L)
case class NetworkCommandResult(
  requestId: String,
  commandStatus: String,
  consoleOutput: String) extends NotebookResult with Serializable

/**
 * Use this composite object to store the entire transaction
 * @param notebookId The notebookId
 * @param paragraphId The paragraphId
 * @param command The commands (for the paragraph)
 * @param requestId The requestId (for tracing)
 * @param userId The userId (associated with the request)
 * @param results The collection of command level results (for supporting multiple commands)
 */
@SerialVersionUID(3L)
case class NotebookExecutionDetails(
  notebookId: String,
  paragraphId: String,
  command: String,
  requestId: String,
  userId: Option[String],
  results: Seq[NetworkCommandResult]) extends NotebookRequest with Serializable



