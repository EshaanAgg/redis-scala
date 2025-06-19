package redis.handler.commands

import redis.Connection
import redis.Role.Master
import redis.Role.Slave
import redis.ServerState
import redis.formats.RESPData
import redis.formats.RESPData.Array as RESPArray
import redis.formats.RESPData.BulkString
import redis.handler.Handler

import java.time.Instant
import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object WaitHandler extends Handler:
  val getAckRESP: RESPArray = RESPArray(
    BulkString("REPLCONF"),
    BulkString("GETACK"),
    BulkString("*")
  )

  /** Sends a "REPLCONF GETACK" command to the replica and waits for the
    * response. If the response is valid, it extracts the ACK value and returns
    * it. If the response is invalid or an error occurs, it logs an error
    * message and returns None.
    * @param c
    */
  private def getACKFromReplica(c: Connection): Option[Int] =
    def logError(msg: String): Unit =
      println(s"[WAIT] [GETACK :${c.port}] ${msg}")

    c.sendAndTryResponse(getAckRESP) match
      case Success(RESPArray(Some(arr))) =>
        if arr.length == 3 && arr(0) == BulkString("REPLCONF") &&
          arr(1) == BulkString("ACK")
        then
          arr(2) match
            case BulkString(v) =>
              v match
                case Some(str) => Some(str.toInt)
                case None =>
                  logError("ACK value is NULL")
                  None
            case _ =>
              logError(s"Expected BulkString for ACK, received ${arr(2)}")
              None
        else
          logError(s"Invalid GETACK response format: ${arr.mkString(", ")}")
          None
      case other =>
        logError(s"Invalid response recieved: ${other}")
        None

  /** Recursive handler to wait for a minimum number of replicas to acknowledge
    * the streamed offset. When this fucntion finally returns, it adds the
    * offset for all the "REPLCONF GETACK" commands sent to the replicas to the
    * `streamedOffset` of the master instance.
    * @param minReplicaCnt
    *   Minimum number of replicas that should acknowledge
    * @param timeout
    *   The time until which to wait for the replicas
    * @param m
    * @param lastReplicaCount
    * @param callCount
    * @return
    *   A Try containing the number of replicas that acknowledged or an error if
    *   the timeout is reached or an issue occurs
    */
  @tailrec
  private def hander(
      minReplicaCnt: Int,
      timeout: Instant,
      m: Master,
      lastReplicaCount: Int = 0,
      callCount: Int = 0
  ): Try[RESPData] =
    if Instant.now().isAfter(timeout)
    then
      m.streamedOffset += callCount * getAckRESP.getBytes.length // Update the streamed offset
      Success(RESPData.Integer(lastReplicaCount)) // Time reached
    else
      // Check the current replica count
      val newReplicaCount = m.replicas
        .map(getACKFromReplica)
        .count(_.getOrElse(-1) >= m.streamedOffset)
      println(
        s"[WAIT] Replica Count: $newReplicaCount - Min Required: $minReplicaCnt"
      )

      if newReplicaCount >= minReplicaCnt || Instant.now().isAfter(timeout)
      then
        m.streamedOffset += callCount * getAckRESP.getBytes.length // Update the streamed offset
        Success(RESPData.Integer(newReplicaCount))
      else
        Thread.sleep(50)
        hander(minReplicaCnt, timeout, m, newReplicaCount, callCount + 1)

  def handle(args: Array[String]): Try[RESPData] =
    ServerState.role match
      case m: Master =>
        if args.length != 3
        then
          Failure(
            IllegalArgumentException(
              s"Expected exactly 3 arguments to 'WAIT', received ${args.mkString("Array(", ",", ")")}"
            )
          )
        else
          Try {
            val replicaCnt = args(1).toInt
            val timeout = Instant.now().plusMillis(args(2).toInt)
            hander(replicaCnt, timeout, m)
          }.flatten

      case _: Slave =>
        Failure(Exception("Wait command cannot be executed by a slave"))
