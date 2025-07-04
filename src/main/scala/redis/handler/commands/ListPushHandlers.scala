package redis.handler.commands

import redis.ServerState
import redis.ds.LinkedList
import redis.formats.RESPData
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

def validateLPushArgs(args: Array[String], opName: String): Option[String] =
  if args.length < 3
  then
    Some(
      s"Expected at least 2 arguments to '$opName' (list and value to add), received ${args.mkString("Array(", ", ", ")")}"
    )
  else None

object ListPushHandler:
  private type mapperFn = (LinkedList[String], String) => Unit

  private def getResult(
    cmdName: String,
    fn: mapperFn,
    args: Array[String]): Try[RESPData] =
    validateLPushArgs(args, cmdName) match
      case Some(error) => Failure(IllegalArgumentException(error))
      case None =>
        val list = ServerState.getOrCreateList(args(1))
        args.drop(2).foreach(fn(list, _))
        Success(RESPData.Integer(list.length))

  object LPush extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      getResult("LPUSH", (l, v) => l.addHead(v), args)

  object RPush extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      getResult("RPUSH", (l, v) => l.addTail(v), args)
