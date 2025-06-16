package redis.handler.commands

import redis.RESP2.DataType
import redis.RESP2.DataType.BulkString
import redis.ServerState
import redis.StoreVal
import redis.handler.Handler

import java.time.Instant
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object SetHandler extends Handler:
  def handle(args: Array[String]): Try[DataType] =
    if args.length != 3 && args.length != 5
    then
      Failure(
        IllegalArgumentException(
          s"Invalid arguments provided to 'SET', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      val key = args(1)
      val data = BulkString(args(2))
      val expiry =
        if args.length == 5
        then Some(Instant.now().plusMillis(args(4).toInt))
        else None

      ServerState.addKey(key, StoreVal(data, expiry))
      Success(DataType.SimpleString("OK"))
