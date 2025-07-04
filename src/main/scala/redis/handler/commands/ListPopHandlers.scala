package redis.handler.commands

import redis.ServerState
import redis.ds.LinkedList
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Args(
  key: String,
  cnt: Option[Int]
)

def validateLPopArgs(
  args: Array[String],
  opName: String): Either[String, Args] =
  if args.length != 2 && args.length != 3
  then
    Left(
      s"Invalid arguments to '$opName': ${args.mkString("Array(", ", ", ")")}"
    )
  else
    Right(
      Args(
        args(1),
        if args.length == 3 then Some(args(2).toInt) else None
      )
    )

object ListPopHandler:
  private type mapperFn = LinkedList[String] => String

  private def getResult(
    cmdName: String,
    fn: mapperFn,
    args: Array[String]): Try[RESPData] =
    validateLPopArgs(args, cmdName) match
      case Left(error) => Failure(IllegalArgumentException(error))
      case Right(args) =>
        val list = ServerState.getOrCreateList(args.key)
        Success(
          if list.isEmpty
          then BulkString.Null
          else
            args.cnt match
              case None => BulkString(fn(list))
              case Some(cnt) =>
                RESPData.Array(
                  (1 to math.min(list.length, cnt))
                    .map(_ => BulkString(fn(list)))
                    .toList
                )
        )

  object LPop extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      getResult("LPOP", l => l.popHead, args)

  object RPop extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      getResult("LPOP", l => l.popTail, args)
