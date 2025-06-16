package redis.handler.commands

import redis.RESP2.DataType
import redis.RESP2.DataType.BulkString
import redis.RESP2.DataType.{Array => RESPArray}
import redis.ServerState
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object KeysHandler extends Handler:
  def handle(args: Array[String]): Try[DataType] =
    if args.length != 2
    then
      Failure(
        IllegalArgumentException(
          s"Expected 2 arguments to 'KEYS', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else if args(1).toUpperCase != "\"*\""
    then
      Failure(
        IllegalArgumentException(
          s"Only '*' (all keys) option is supported currently, can't filter by the pattern ${args(1)}"
        )
      )
    else
      Success(
        RESPArray(
          ServerState.allKeys.map(BulkString(_)).toList
        )
      )
