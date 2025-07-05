package redis.handler.commands

import redis.ServerState
import redis.ds.LinkedList
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import redis.handler.Handler

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class BlockPopArgs(
  keys: List[String],
  timeout: Double
):
  def timeoutReached(startTime: Long): Boolean =
    timeout != 0.0 && System.currentTimeMillis() - startTime >= timeout * 1000

def validateBlockPopArgs(
  args: Array[String],
  opName: String
): Either[String, BlockPopArgs] =
  if args.length < 3
  then
    Left(
      s"Expected at least 2 arguments to '$opName' (list key and timeout), received ${args.mkString("Array(", ", ", ")")}"
    )
  else
    Right(
      BlockPopArgs(
        args.drop(1).dropRight(1).toList,
        args.last.toDouble
      )
    )

// BLOCKING commands -> Must release the lock on the ServerState mutex
// manually if no error occurs in the handler
object ListBlockPopHandler:
  private type mapperFn = LinkedList[String] => String

  /** Iterates over the keys and checks if any of them have a non-empty list. If
    * a non-empty list is found, it pops the head or tail based on the provided
    * function `fn` and returns the key and value as a tuple. The lock on the
    * ServerState shpuld be managed by the caller.
    *
    * @param keys
    * @param fn
    * @return
    *   Option containing the key and value if a non-empty list is found, or
    *   None if all lists are empty.
    */
  private def checkForPop(
    keys: List[String],
    fn: mapperFn
  ): Option[(String, String)] =
    keys.view
      .collectFirst {
        case key if ServerState.hasNonEmptyList(key) =>
          val list = ServerState.getOrCreateList(key)
          (key, fn(list))
      }

  /** Creates a RESPData response from the key-value pair. Also releases the
    * lock on ServerState after the blocking operation
    *
    * @param kv
    *   The key-value pair to create the response from
    * @return
    *   Success containing the RESPData response
    */
  private def createResponse(
    kv: (String, String)
  ): Success[RESPData] =
    ServerState.mtx.unlock()
    Success(RESPData.Array(List(BulkString(kv._1), BulkString(kv._2))))

  private def waitForResponse(
    args: BlockPopArgs,
    fn: mapperFn,
    startTime: Long = System.currentTimeMillis(),
    waitTimeMS: Long = 20
  ): RESPData =
    ServerState.mtx.lock()
    checkForPop(args.keys, fn) match // Check for new elements
      case Some(kv) =>
        createResponse(kv).get
      case None =>
        ServerState.mtx.unlock() // Release the lock to allow other operations
        if args.timeoutReached(startTime)
        then RESPData.BulkString.Null
        else
          Thread.sleep(
            waitTimeMS
          ) // Wait for a short duration before checking again
          waitForResponse(args, fn, startTime)

  private def getResult(
    cmdName: String,
    fn: mapperFn,
    args: Array[String]
  ): Try[RESPData] =
    validateBlockPopArgs(args, cmdName) match
      case Left(error) => Failure(IllegalArgumentException(error))
      case Right(args) =>
        // Check intially for any elements
        // If found, then we do not need to block and can return immediately
        checkForPop(args.keys, fn) match
          case Some(kv) => createResponse(kv)
          case None =>
            ServerState.mtx.unlock()
            Success(waitForResponse(args, fn))

  object LPop extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      getResult("BLPOP", l => l.popHead, args)

  object RPop extends Handler:
    def handle(args: Array[String]): Try[RESPData] =
      getResult("BLPOP", l => l.popTail, args)
