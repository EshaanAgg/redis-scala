package redis.handler.commands

import redis.Connection
import redis.Role.Master
import redis.Role.Slave
import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.Array as RESPArray
import redis.formats.RESPData.BulkString
import redis.formats.RESPData.SimpleString
import redis.handler.HandlerWithConnection

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ReplconfHandler extends HandlerWithConnection:
  private def slaveHander(args: Array[String], sl: Slave): Try[RESPData] =
    // Slave can only execute the 'REPLCONF GETACK *' command
    if args.length == 3 && args(1).toLowerCase == "getack" && args(2) == "*"
    then
      Success(
        RESPArray(
          BulkString("REPLCONF"),
          BulkString("ACK"),
          BulkString(sl.acknowledgedOffset.toString)
        )
      )
    else
      Failure(
        IllegalArgumentException(
          s"Slave can only execute REPLCONF GETACK command, received ${args.mkString("Array(", ",", ")")}"
        )
      )

  private def masterHander(
      args: Array[String],
      m: Master,
      conn: Connection
  ): Try[RESPData] =
    if args.length < 3
    then
      Failure(
        IllegalArgumentException(
          s"Expected atleast 3 arguments to 'REPLCONF', received ${args.mkString("Array(", ",", ")")}"
        )
      )
    // Handle listening-port command
    else if args(1).toLowerCase == "listening-port"
    then
      println(s"[Registered Replica] :${args(2)}")
      m.replicas += conn
      Success(SimpleString("OK"))

    // Handle capa command
    else if args(1).toLowerCase == "capa"
    then
      println(
        s"[Replica Capabilites] ${args.drop(2).mkString(",")}"
      )
      Success(SimpleString("OK"))

    // Handle unknown commands
    else
      Failure(
        IllegalArgumentException(
          s"Unknown REPLCONF command: ${args.mkString("Array(", ", ", ")")}"
        )
      )

  def handle(args: Array[String], conn: Connection): Try[RESPData] =
    ServerState.role match
      case s: Slave  => slaveHander(args, s)
      case m: Master => masterHander(args, m, conn)
