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
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object WaitHandler extends Handler:
  val getAckRESP: RESPArray = RESPArray(
    BulkString("REPLCONF"),
    BulkString("GETACK"),
    BulkString("*")
  )

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

  private def hander(
      minReplicaCnt: Int,
      timeout: Instant,
      m: Master,
      lastReplicaCount: Int = 0
  ): Try[RESPData] =
    if Instant.now().isAfter(timeout)
    then Success(RESPData.Integer(lastReplicaCount)) // Time reached
    else
      // Check the current replica count
      val newReplicaCount = m.replicas
        .map(getACKFromReplica)
        .count(_.getOrElse(-1) >= m.streamedOffset)
      println(
        s"[WAIT] Replica Count: $newReplicaCount - Min Required: $minReplicaCnt"
      )
      if newReplicaCount >= minReplicaCnt
      then Success(RESPData.Integer(newReplicaCount))
      else
        // Wait for 50ms and retry
        Thread.sleep(50)
        hander(minReplicaCnt, timeout, m, newReplicaCount)

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
