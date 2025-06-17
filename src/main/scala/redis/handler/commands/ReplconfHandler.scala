package redis.handler.commands

import redis.Connection
import redis.Role.Master
import redis.Role.Slave
import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.SimpleString
import redis.handler.HandlerWithConnection

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ReplconfHandler extends HandlerWithConnection:
  def handle(args: Array[String], conn: Connection): Try[RESPData] =
    ServerState.role match
      case Slave(_, _) =>
        Failure(
          IllegalStateException(
            "REPLCONF command cannot be executed by a slave server"
          )
        )
      case Master(_, _, replicas) =>
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
          replicas :+ conn
          Success(SimpleString("OK"))

        // Handle capa command
        else if args(1).toLowerCase == "capa"
        then
          println(s"[Replica Capabilites] ${args.drop(2).mkString(",")}")
          Success(SimpleString("OK"))

        // Handle unknown commands
        else
          Failure(
            IllegalArgumentException(
              s"Unknown REPLCONF command: ${args.mkString("Array(", ", ", ")")}"
            )
          )
