package redis.handler.commands

import redis.Role.Master
import redis.Role.Slave
import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.SimpleString
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object PsyncHandler extends Handler:
  def handle(args: Array[String]): Try[RESPData] =
    ServerState.role match
      case Slave(_, _) =>
        Failure(
          IllegalStateException(
            "PSYNC command cannot be executed by a slave server"
          )
        )
      case Master(id, offset, _) =>
        if args.length != 3 || args(1).toLowerCase() != "?" || args(2)
            .toLowerCase() != "-1"
        then
          Failure(
            IllegalArgumentException(
              s"Expected the command to be 'PSYNC ? -1', received ${args.mkString("Array(", ",", ")")}"
            )
          )
        else Success(SimpleString(s"FULLRESYNC $id $offset"))
