package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object LRangeHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 4
    then
      Failure(
        IllegalArgumentException(
          s"Expected 3 arguments to 'LRANGE' (key startIdx endIdx), received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      val l = ServerState.getOrCreateList(args(1))
      val stIdx = args(2).toInt
      val entries =
        if stIdx >= l.length || l.isEmpty then Nil
        else
          val idx1 = l.getNormalizedIdx(stIdx)
          val idx2 = l.getNormalizedIdx(args(3).toInt)
          if idx1 <= idx2
          then l(l(idx1), l(idx2))
          else Nil

      Success(RESPData.Array(entries.map(e => BulkString(e.value))))
