package redis.handler.commands

import redis.RESP2.DataType
import redis.handler.Handler

import scala.util.Failure
import scala.util.Try

object UnknownHandler extends Handler:
  def handle(args: Array[String]): Try[DataType] =
    Failure(IllegalArgumentException(s"Unrecognized command ${args(0)}"))
