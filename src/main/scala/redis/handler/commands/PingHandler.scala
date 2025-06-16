package redis.handler.commands

import redis.RESP2.DataType
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object PingHandler extends Handler:
  def handle(args: Array[String]): Try[DataType] =
    if args.length != 1
    then
      Failure(
        IllegalArgumentException(
          s"Expected no arguments to 'PING', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else Success(DataType.SimpleString("PONG"))
