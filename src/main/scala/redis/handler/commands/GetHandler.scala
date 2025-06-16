package redis.handler.commands

import redis.RESP2.DataType
import redis.ServerState
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object GetHandler extends Handler:
  def handle(args: Array[String]): Try[DataType] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 1 arguments to 'GET', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      ServerState.get(args(1)) match
        case None    => Success(DataType.BulkString(None))
        case Some(v) => Success(v.data)
