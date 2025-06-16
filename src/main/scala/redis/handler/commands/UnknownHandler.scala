package redis.handler.commands

import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Try

object UnknownHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    Failure(IllegalArgumentException(s"Unrecognized command ${args(0)}"))
