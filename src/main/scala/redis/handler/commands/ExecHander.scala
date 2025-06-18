package redis.handler.commands

import redis.Connection
import redis.formats.RESPData
import redis.handler.Handler
import redis.handler.HandlerWithConnection

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ExecHandler extends HandlerWithConnection:
  /** Makes use of the Handler's handlerMap to process the commands. TODO: Add
    * proper handling for other commands that might be queued.
    * @param args
    * @return
    */
  private def processCommand(args: Array[String]): RESPData =
    val tryRESP = Handler.handlerMap.get(args(0).toLowerCase) match
      case Some(handler) => handler.handle(args)
      case None          => UnknownHandler.handle(args)

    tryRESP match
      case Success(resp) => resp
      case Failure(e)    => RESPData.Error(e.getMessage)

  def handle(args: Array[String], conn: Connection): Try[RESPData] =
    if args.length != 1
    then
      Failure(
        IllegalArgumentException(
          s"'EXEC' takes no arguments, received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else if !conn.inTransaction then
      Success(RESPData.Error("ERR EXEC without MULTI"))
    else
      conn.inTransaction = false
      val response = RESPData.Array(
        conn.queuedCommands.map(processCommand).toList
      )
      conn.queuedCommands.clear()
      Success(response)
