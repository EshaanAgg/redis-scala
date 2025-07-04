package redis.handler.commands

import redis.ServerState
import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

def validateArgs(args: Array[String], opName: String): Option[String] =
  if args.length < 3
  then
    Some(
      s"Expected at least 2 arguments to '$opName' (list and value to add), received ${args.mkString("Array(", ", ", ")")}"
    )
  else None

object ListPushHandler:

  object LPush extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      validateArgs(args, "LPUSH") match
        case Some(error) => Failure(IllegalArgumentException(error))
        case None =>
          val list = ServerState.getOrCreateList(args(1))
          args.drop(2).foreach(list.addHead)
          Success(RESPData.Integer(list.length))

  object RPush extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      validateArgs(args, "RPUSH") match
        case Some(error) => Failure(IllegalArgumentException(error))
        case None =>
          val list = ServerState.getOrCreateList(args(1))
          args.drop(2).foreach(list.addTail)
          Success(RESPData.Integer(list.length))
