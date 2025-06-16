package redis.handler.commands

import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import redis.formats.RESPData.BulkString
import redis.ServerState
import redis.Role.Master
import redis.Role.Slave
import redis.Role

object InfoHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 2 || args(1).toLowerCase != "replication"
    then
      Failure(
        IllegalArgumentException(
          s"Expected command to be 'INFO replication', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else 
      val kvPairs: Seq[(String, String)] = Role.getInfoEntries(ServerState.role)

      Success(BulkString(
        kvPairs.map((k, v) => s"$k:$v").mkString("\n")
      ))
