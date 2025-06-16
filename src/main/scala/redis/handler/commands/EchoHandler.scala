package redis.handler.commands

import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object EchoHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 1 arguments to 'ECHO', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else Success(RESPData.BulkString(args(1)))
