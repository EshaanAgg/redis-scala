package redis.handler.commands

import redis.Entry
import redis.StreamStore
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object XaddHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length < 3 || args.length % 2 != 1 then
      Failure(
        IllegalArgumentException(
          s"Invalid arguments to 'XADD', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      val streamName = args(1)
      Success(
        Entry(args.drop(1)) match
          case Left(err) => RESPData.Error(err)
          case Right(entry) =>
            StreamStore.addEntryToStream(streamName, entry)
            BulkString(entry.id.toString)
      )
