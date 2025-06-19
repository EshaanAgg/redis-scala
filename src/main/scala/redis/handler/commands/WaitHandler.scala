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
  /** Recursive handler to wait for a minimum number of replicas to acknowledge
    * the streamed offset.
    * @param minReplicaCnt
    *   Minimum number of replicas that should acknowledge
    * @param timeout
    *   The time until which to wait for the replicas
    * @param m
    * @param lastReplicaCount
    * @return
    *   A Try containing the number of replicas that acknowledged or an error if
    *   the timeout is reached or an issue occurs
    */
  @tailrec
  private def hander(
      minReplicaCnt: Int,
      timeout: Instant,
      m: Master,
      lastReplicaCount: Int = 0
  ): Try[RESPData] =
    if Instant.now().isAfter(timeout)
    then Success(RESPData.Integer(lastReplicaCount)) // Timeou reached
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
      then Success(RESPData.Integer(newReplicaCount))
      else
        Thread.sleep(100)
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
