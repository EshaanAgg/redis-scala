package redis

import redis.formats.RESPData.Array as RESPArray
import redis.formats.RESPData.BulkString
import redis.formats.RESPData.SimpleString

import scala.collection.mutable.ArraySeq

sealed trait Role:
  def performHandshake: Unit

object Role:
  case class Master(
      replID: String,
      replOffset: Long,
      replicas: ArraySeq[Connection]
  ) extends Role:
    override def performHandshake: Unit =
      println(
        s"[Handshake] Master role with replID: $replID and replOffset: $replOffset"
      )

  case class Slave(masterHost: String, masterPort: Int) extends Role:
    var masterReplID: String = "?"
    var masterReplOffset: Long = -1L

    override def performHandshake: Unit =
      val conn = new Connection(masterHost, masterPort)

      // Step 1: Send a PING message to verify connection
      conn.sendAndExpectResponse(
        RESPArray(BulkString("PING")),
        SimpleString("PONG")
      )

      // Step 2: Send REPLCONF messages to set configuration
      conn.sendAndExpectResponse(
        RESPArray(
          BulkString("REPLCONF"),
          BulkString("listening-port"),
          BulkString(ServerState.port.toString)
        ),
        SimpleString("OK")
      )
      conn.sendAndExpectResponse(
        RESPArray(
          BulkString("REPLCONF"),
          BulkString("capa"),
          BulkString("psync2")
        ),
        SimpleString("OK")
      )

      // Step 3: Send PSYNC command to initiate replication
      val resp = conn.sendAndGetResponse(
        RESPArray(
          BulkString("PSYNC"),
          BulkString(masterReplID),
          BulkString(masterReplOffset.toString)
        )
      )
      // The response should be a simple string of the form:
      // "FULLRESYNC <replid> <offfset>"
      resp match
        case SimpleString(s) if s.startsWith("FULLRESYNC ") =>
          val parts = s.split(" ")
          if parts.length == 3 then
            masterReplID = parts(1)
            masterReplOffset = parts(2).toLong
            println(
              s"[Handshake] Slave role with master replID: $masterReplID and replOffset: $masterReplOffset"
            )
          else throw new Exception(s"Invalid FULLRESYNC response: $s")
        case _ =>
          throw new Exception(s"Unexpected response from PSYNC: $resp")

  def getInfoEntries(role: Role): Seq[(String, String)] =
    role match
      case Master(replID, replOffset, _) =>
        Seq(
          "role" -> "master",
          "master_replid" -> replID,
          "master_repl_offset" -> replOffset.toString
        )
      case Slave(masterHost, masterPort) =>
        Seq(
          "role" -> "slave"
        )
