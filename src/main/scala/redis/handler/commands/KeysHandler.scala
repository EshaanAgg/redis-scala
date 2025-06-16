package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import redis.formats.RESPData.{Array => RESPArray}
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object KeysHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
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
          ServerState.keys.map(BulkString(_)).toList
        )
      )
