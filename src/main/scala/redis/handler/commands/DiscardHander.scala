package redis.handler.commands

import redis.Connection
import redis.formats.RESPData
import redis.handler.HandlerWithConnection

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object DiscardHandler extends HandlerWithConnection:
  def handle(args: Array[String], conn: Connection): Try[RESPData] =
    if args.length != 1
    then
      Failure(
        IllegalArgumentException(
          s"'DISCARD' takes no arguments, received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else if !conn.inTransaction then
      Success(RESPData.Error("ERR DISCARD without MULTI"))
    else
      conn.inTransaction = false
      conn.queuedCommands.clear()
      Success(RESPData.SimpleString("OK"))
