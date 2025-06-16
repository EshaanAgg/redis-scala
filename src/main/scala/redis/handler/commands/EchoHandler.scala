package redis.handler.commands

import redis.RESP2.DataType
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object EchoHandler extends Handler:
  def handle(args: Array[String]): Try[DataType] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 1 arguments to 'ECHO', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else Success(DataType.BulkString(args(1)))
