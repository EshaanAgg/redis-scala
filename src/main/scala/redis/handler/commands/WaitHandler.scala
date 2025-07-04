package redis.handler.commands

import redis.Role.Master
import redis.Role.Slave
import redis.ServerState
import redis.formats.RESPData
import redis.handler.Handler

import java.time.Instant
import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object WaitHandler extends Handler:
  val getACKMessageSize = 37

  /** Recursive handler to wait for a minimum number of replicas to acknowledge
    * the streamed offset.
    * @param minReplicaCnt
    *   Minimum number of replicas that should acknowledge
    * @param timeout
    *   The time until which to wait for the replicas
    * @param m
    * @param lastReplicaCount
    *   The last known count of replicas that have acknowledged the streamed
    *   offset
    * @param calledCount
    *   The number of times the handler has been called recursively
    * @return
    *   A Try containing the number of replicas that acknowledged or an error if
    *   the timeout is reached or an issue occurs
    */
  @tailrec
  private def hander(
    minReplicaCnt: Int,
    timeout: Instant,
    m: Master,
    lastReplicaCount: Int,
    calledCount: Int = 0
  ): Try[RESPData] =
    // Base case: If no write messages have been issued, then we can assume
    // that all the replicas have acknowledged the same.
    if m.streamedOffset == 0
    then Success(RESPData.Integer(m.replicas.size))
    // If the timeout is reached or the last known replica count is greater than
    else if Instant.now().isAfter(timeout) || lastReplicaCount >= minReplicaCnt
    then
      m.streamedOffset += calledCount * getACKMessageSize
      Success(RESPData.Integer(lastReplicaCount))
    else
      // Check the current replica count by sending the "REPLCONF GETACK" command
      // and then waiting for the responses
      m.replicas.foreach(_.sendGetAckRequest)
      Thread.sleep(100)
      val newReplicaCount =
        m.replicas.count(_.acknowledgedOffset >= m.streamedOffset)
      println(
        s"[WAIT] [Replica Count] Current: $newReplicaCount, Minimum Need: $minReplicaCnt"
      )

      if newReplicaCount >= minReplicaCnt || Instant.now().isAfter(timeout)
      then
        m.streamedOffset += (calledCount + 1) * getACKMessageSize
        Success(RESPData.Integer(newReplicaCount))
      else
        Thread.sleep(100) // Wait a bit before checking again
        hander(minReplicaCnt, timeout, m, newReplicaCount, calledCount + 1)

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
            println(s"[WAIT] Master Offset: ${m.streamedOffset}")
            hander(
              replicaCnt,
              timeout,
              m,
              m.replicas.count(_.acknowledgedOffset >= m.streamedOffset)
            )
          }.flatten

      case _: Slave =>
        Failure(Exception("Wait command cannot be executed by a slave"))
