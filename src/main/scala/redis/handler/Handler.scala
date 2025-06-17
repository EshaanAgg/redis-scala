package redis.handler

import redis.Connection
import redis.formats.RESPData
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

import java.net.Socket
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
  val handlerMap: Map[String, Handler] = Map(
    "ping" -> PingHandler,
    "echo" -> EchoHandler,
    "set" -> SetHandler,
    "get" -> GetHandler,
    "config" -> ConfigHandler,
    "keys" -> KeysHandler,
    "info" -> InfoHandler,
    "psync" -> PsyncHandler
  )

  val handlerWithConnectionMap: Map[String, HandlerWithConnection] = Map(
    "replconf" -> ReplconfHandler
  )

  val postMessageHandlers: Map[String, PostMessageHandler] = Map(
  )

  /** Processes the command received from the client connection, and sends the
    * appropriate response back to the client.
    *
    * @param args
    *   Array of command arguments.
    * @param conn
    *   Connection object representing the client connection.
    */
  private def sendResponse(args: Array[String], conn: Connection): Unit =
    val response = args(0) match
      case x if handlerMap.contains(x.toLowerCase) =>
        handlerMap(x.toLowerCase).handle(args)
      case x if handlerWithConnectionMap.contains(x.toLowerCase) =>
        handlerWithConnectionMap(x.toLowerCase).handle(args, conn)
      case _ => UnknownHandler.handle(args)

    conn.sendData(
      response match
        case Success(data) => data
        case Failure(err)  => RESPData.Error(err.toString)
    )

  /** Runs the appropriate post message handler based on the command received.
    * @param args
    * @param conn
    */
  private def postMessage(args: Array[String], conn: Connection): Unit =
    args(0) match
      case x if postMessageHandlers.contains(x.toLowerCase) =>
        postMessageHandlers(x.toLowerCase).handle(args, conn)
      case _ => ()

  /** Processes the incoming connection by reading the command from the input
    * stream, sending the response back to the client, and handling any
    * post-message actions.
    *
    * @param conn
    *   Connection object representing the client connection.
    */
  private def process(conn: Connection): Unit =
    val in = conn.inputStream
    Parser.getCommand(in) match
      case Success(args) =>
        if args.nonEmpty then
          sendResponse(args, conn)
          postMessage(args, conn)
      case Failure(err) =>
        val errorMessage = RESPData.Error(err.toString)
        conn.sendBytes(errorMessage.getBytes)

  def socketHandler(socket: Socket): Unit =
    val conn = Connection(socket)

    while (!conn.isClosed)
      process(conn)
