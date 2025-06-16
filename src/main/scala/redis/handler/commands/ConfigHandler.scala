package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import redis.formats.RESPData.{Array => RESPArray}
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ConfigHandler extends Handler:
  private val supportedConfigs = Seq("dir", "dbfilename")

  def handle(args: Array[String]): Try[RESPData] =
    if args.length != 3
    then
      Failure(
        IllegalArgumentException(
          s"Expected 3 arguments to 'CONFIG', received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else if args(1).toUpperCase != "GET" || !supportedConfigs.contains(args(2))
    then
      Failure(
        IllegalArgumentException(
          s"Invalid arguments received: expected (CONFIG, GET, ${supportedConfigs.mkString("|")}), received ${args.mkString("Array(", ", ", ")")}"
        )
      )
    else
      Success {
        args(2) match {
          case "dir" =>
            RESPArray(
              BulkString("dir"),
              BulkString(ServerState.dir)
            )
          case "dbfilename" =>
            RESPArray(
              BulkString("dbfilename"),
              BulkString(ServerState.dbFile)
            )
          case _ => throw Exception("unreachable code in ConfigHandler")
        }
      }
