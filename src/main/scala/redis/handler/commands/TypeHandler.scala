package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import redis.formats.RESPData.SimpleString
import redis.StreamStore

object TypeHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 1 arguments to 'TYPE', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      val key = args(1)
      Success(
        ServerState.get(key) match
          case Some(_) => SimpleString("string") 
          case None => 
            if StreamStore.streamExists(key) 
            then SimpleString("stream") 
            else SimpleString("none")
      )
