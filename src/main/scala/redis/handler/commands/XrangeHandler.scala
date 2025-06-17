package redis.handler.commands

import redis.EntryID
import redis.StreamStore
import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object XrangeHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length < 4 then
      Failure(
        IllegalArgumentException(
          s"Invalid arguments to 'XADD', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      val streamName = args(1)
      val startKey = EntryID.forRange(
        if args(2) == "-" then "0-0" else args(2)
      )

      val endKey =
        if args(3) == "+"
        then StreamStore.lastEntry(streamName).getOrElse(EntryID(0, 0))
        else EntryID.forRange(args(3))

      Success(
        RESPData.Array(
          StreamStore
            .getStream(streamName)
            .getOrElse(List())
            .filter(entry => entry.id >= startKey && entry.id <= endKey)
            .map(_.getResp)
            .toList
        )
      )
