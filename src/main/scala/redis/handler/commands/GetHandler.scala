package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object GetHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 1 arguments to 'GET', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      ServerState.get(args(1)) match
        case None    => Success(RESPData.BulkString(None))
        case Some(v) => Success(v.data)
