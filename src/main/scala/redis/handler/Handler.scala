package redis.handler

import redis.Connection
import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.Array as RESPArray
import redis.formats.RESPData.BulkString
import redis.handler.commands.ConfigHandler
import redis.handler.commands.EchoHandler
import redis.handler.commands.GetHandler
import redis.handler.commands.InfoHandler
import redis.handler.commands.KeysHandler
import redis.handler.commands.PingHandler
import redis.handler.commands.PsyncHandler
import redis.handler.commands.ReplconfHandler
import redis.handler.commands.SetHandler
import redis.handler.commands.UnknownHandler
import redis.handler.postHandlers.PsyncPostHandler

import java.io.InputStream
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Handler:
  def handle(args: Array[String]): Try[RESPData]
trait PostMessageHandler:
  def handle(args: Array[String], conn: Connection): Unit

object Handler:
  val writeCommands: Set[String] = Set("SET", "INCR")

  val handlerMap: Map[String, Handler] = Map(
    "ping" -> PingHandler,
    "echo" -> EchoHandler,
    "set" -> SetHandler,
    "get" -> GetHandler,
    "config" -> ConfigHandler,
    "keys" -> KeysHandler,
    "info" -> InfoHandler,
    "psync" -> PsyncHandler,
    "replconf" -> ReplconfHandler
  )

  val postMessageHandlers: Map[String, PostMessageHandler] = Map(
    "psync" -> PsyncPostHandler
  )

  /** Processes the command received from the client connection, and sends the
    * appropriate response back to the client.
    *
    * @param args
    *   Array of command arguments.
    * @param conn
    *   Connection object representing the client connection.
    */
  private def sendResponse(args: Array[String], conn: Connection): Boolean =
    println(s"Received command: ${args.mkString("(", ", ", ")")}")
    val response = args(0) match
      case x if handlerMap.contains(x.toLowerCase) =>
        handlerMap(x.toLowerCase).handle(args)
      case _ => UnknownHandler.handle(args)

    if conn.shouldSendCommandResult(args) then
      conn.sendData(
        response match
          case Success(data) => data
          case Failure(err)  => RESPData.Error(err.toString)
      )

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

  /** If master, then stream all the recieved write commands to all the
    * connected replicas.
    * @param bytes
    * @return
    */
  private def streamToReplicas(args: Array[String]) =
    if writeCommands.contains(args(0).toUpperCase) then
      val bytes = RESPArray(args.map(BulkString(_)).toList).getBytes
      ServerState.replicas.foreach(_.sendBytes(bytes))

  /** Processes the incoming connection by reading the command from the input
    * stream, sending the response back to the client, and handling any
    * post-message actions.
    *
    * @param conn
    *   Connection object representing the client connection.
    */
  def connectionHandler(in: InputStream, conn: Connection): Unit =
    Parser.getCommand(in) match
      case Success(args) =>
        if args.nonEmpty then
          streamToReplicas(args)
          // Send response to the client, and if the same is successful,
          // run the post message handler if applicable.
          if sendResponse(args, conn)
          then postMessage(args, conn)
      case Failure(err) =>
        val errorMessage = RESPData.Error(err.toString)
        conn.sendBytes(errorMessage.getBytes)
