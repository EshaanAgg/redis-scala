package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object IncrHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 1 argument to 'INCR', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      Success(
        ServerState.incr(args(1)) match
          case Left(err) => RESPData.Error(err)
          case Right(v)  => RESPData.Integer(v)
      )
