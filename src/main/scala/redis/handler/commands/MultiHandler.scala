package redis.handler.commands

import redis.Connection
import redis.formats.RESPData
import redis.handler.HandlerWithConnection

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object MultiHandler extends HandlerWithConnection:
  def handle(args: Array[String], conn: Connection): Try[RESPData] =
    if args.length != 1
    then
      Failure(
        IllegalArgumentException(
          s"'MULTI' takes no arguments, received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else if conn.inTransaction then
      Success(RESPData.Error("ERR MULTI calls can not be nested"))
    else
      conn.inTransaction = true
      conn.queuedCommands.clear()
      Success(RESPData.SimpleString("OK"))
