package redis.handler

import redis.Connection
import redis.Role.Master
import redis.Role.Slave
import redis.ServerState
import redis.formats.Decoder
import redis.formats.RESPData
import redis.formats.RESPData.Array as RESPArray
import redis.formats.RESPData.BulkString
import redis.handler.commands as cmd
import redis.handler.postHandlers as postCmd

import java.io.IOException
import java.time.Instant
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Handler:
  def handle(args: Array[String]): Try[RESPData]

trait HandlerWithConnection:
  def handle(args: Array[String], conn: Connection): Try[RESPData]

trait PostMessageHandler:
  def handle(args: Array[String], conn: Connection): Unit

object Handler:
  val writeCommands: Set[String] = Set("SET", "INCR", "XADD")

  val handlerMap: Map[String, Handler] = Map(
    "ping" -> cmd.PingHandler,
    "echo" -> cmd.EchoHandler,
    "set" -> cmd.SetHandler,
    "get" -> cmd.GetHandler,
    "config" -> cmd.ConfigHandler,
    "keys" -> cmd.KeysHandler,
    "info" -> cmd.InfoHandler,
    "psync" -> cmd.PsyncHandler,
    "type" -> cmd.TypeHandler,
    "xadd" -> cmd.XaddHandler,
    "xrange" -> cmd.XrangeHandler,
    "xread" -> cmd.XreadHandler,
    "incr" -> cmd.IncrHandler,
    "wait" -> cmd.WaitHandler
  )

  val handlerWithConnectionMap: Map[String, HandlerWithConnection] = Map(
    "replconf" -> cmd.ReplconfHandler,
    "discard" -> cmd.DiscardHandler,
    "multi" -> cmd.MultiHandler,
    "exec" -> cmd.ExecHandler
  )

  val postMessageHandlers: Map[String, PostMessageHandler] = Map(
    "psync" -> postCmd.PsyncPostHandler
  )

  val transactionEndingCommands: Set[String] = Set("EXEC", "DISCARD")

  private def inTransactionHandler(
      args: Array[String],
      conn: Connection
  ): Try[RESPData] =
    // If the transaction is ending, dispatch the correct handler
    if transactionEndingCommands.contains(args(0).toUpperCase)
    then handlerWithConnectionMap(args(0).toLowerCase).handle(args, conn)
    else
      // Else queue it for later execution, and send "QUEUED" response
      conn.queuedCommands += args
      Success(RESPData.SimpleString("QUEUED"))

  /** Processes the command received from the client connection, and sends the
    * appropriate response back to the client.
    *
    * @param args
    *   Array of command arguments.
    * @param conn
    *   Connection object representing the client connection.
    */
  private def sendResponse(args: Array[String], conn: Connection): Boolean =
    val timeSuff = Instant.now().toEpochMilli.toString.drop(7)
    println(
      s"${conn.logPrefix} [${timeSuff} ms] ${args.mkString("(", ", ", ")")}"
    )

    val response =
      if conn.inTransaction
      then inTransactionHandler(args, conn)
      else
        args(0) match
          case x if handlerMap.contains(x.toLowerCase) =>
            handlerMap(x.toLowerCase).handle(args)
          case x if handlerWithConnectionMap.contains(x.toLowerCase) =>
            handlerWithConnectionMap(x.toLowerCase).handle(args, conn)
          case _ => cmd.UnknownHandler.handle(args)

    conn.updateAcknowledgedOffset(args)

    if conn.shouldSendCommandResult(args) then
      val data = response match
        case Success(data) => data
        case Failure(err)  => RESPData.Error(err.toString)
      println(
        s"${conn.logPrefix} [${timeSuff} ms] Response: ${data}"
      )
      conn.sendData(data)

    response.isSuccess

  /** Runs the appropriate post message handler based on the command received.
    * @param args
    * @param conn
    */
  private def postMessage(args: Array[String], conn: Connection): Unit =
    args(0) match
      case x if postMessageHandlers.contains(x.toLowerCase) =>
        postMessageHandlers(x.toLowerCase).handle(args, conn)
      case _ => ()

  /** If master, then stream all the received write commands to all the
    * connected replicas.
    * @param bytes
    * @return
    */
  private def streamToReplicas(args: Array[String]): Unit =
    if writeCommands.contains(args(0).toUpperCase) then
      val bytes = RESPArray(args.map(BulkString(_)).toList).getBytes
      ServerState.role match
        case m: Master =>
          m.streamedOffset += bytes.length
          m.replicas.foreach(_.sendBytes(bytes))
        case _: Slave => () // No actions needed for slaves

  /** Processes the incoming connection by reading the command from the input
    * stream, sending the response back to the client, and handling any
    * post-message actions.
    *
    * @param conn
    *   Connection object representing the client connection.
    */
  def connectionHandler(d: Decoder, conn: Connection): Unit =
    Parser.getCommand(d) match
      case Success(args) =>
        if args.nonEmpty then
          streamToReplicas(args)
          // Send response to the client, and if the same is successful,
          // run the post message handler if applicable.
          if sendResponse(args, conn)
          then postMessage(args, conn)
      case Failure(err) =>
        if err.isInstanceOf[IOException] then
          println(s"${conn.logPrefix} Connection closed")
          conn.disconnect()
        else
          println(
            s"[:${conn.port}] Error processing command: ${err.getMessage}"
          )
          conn.sendData(RESPData.Error(err.getMessage))
