package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.Integer
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object LLenHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 1 arguments to 'LLEN', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      Success(
        Integer(
          ServerState.getOrCreateList(args(1)).length
        )
      )
