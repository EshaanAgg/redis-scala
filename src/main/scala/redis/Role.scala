package redis

import redis.formats.RDBFile
import redis.formats.RESPData
import redis.formats.RESPData.Array as RESPArray
import redis.formats.RESPData.BulkString
import redis.formats.RESPData.SimpleString

import scala.collection.mutable.ListBuffer
import scala.util.Failure
import scala.util.Success

sealed trait Role:
  def performHandshake(): Unit

private val getAckMessage: RESPData = RESPArray(
  BulkString("REPLCONF"),
  BulkString("GETACK"),
  BulkString("*")
)

case class Replica(conn: Connection, var acknowledgedOffset: Long = 0L):
  def sendBytes(bytes: Array[Byte]): Unit =
    conn.sendBytes(bytes)

  def sendGetAckRequest: Unit =
    sendBytes(getAckMessage.getBytes)

object Role:
  case class Master(
      replID: String,
      replOffset: Long,
      replicas: ListBuffer[Replica]
  ) extends Role:
    var streamedOffset: Long = 0L

    override def performHandshake(): Unit =
      println(
        s"[Handshake] Master role with replID: $replID and replOffset: $replOffset"
      )

    def getReplica(conn: Connection): Option[Replica] =
      replicas.find(_.conn == conn)

    /** Removes the connection from the list of replicas (if it exists). This is
      * typically called when a replica disconnects.
      * @param conn
      *   The connection to remove
      */
    def removeReplica(conn: Connection): Unit =
      replicas.indexWhere(_.conn == conn) match
        case -1 => ()
        case idx =>
          replicas.remove(idx)

  // Conn is the connection to the master than is opened by the slave
  case class Slave(masterHost: String, masterPort: Int) extends Role:
    var masterReplID: String = "?"
    var masterReplOffset: Long = -1L
    var acknowledgedOffset: Long = 0L
    val conn: Connection = Connection(masterHost, masterPort, true)

    // Perform the handshake with the master server
    // and then register the connection handler for all subsequent messages
    override def performHandshake(): Unit =
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
          throw new Exception(
            s"Unexpected response from PSYNC: $resp"
          )

      // Step 4: Should receive RDB file from the master
      RESPData.getRBDFileContent(conn.d) match
        case Success(rdbBytes) =>
          println(
            s"[Handshake] [RDB File] Received ${rdbBytes.length} bytes -> ${rdbBytes.take(10).mkString("[", ",", "]")}..."
          )
          RDBFile.loadBytes(rdbBytes) match
            case None => println("[Handshake] [RDB File] Load successful")
            case Some(err) =>
              println(s"[Handshake] [RDB File] Load failed: $err")
        case Failure(ex) =>
          println(
            s"[Handshake] [RDB File] Error in getting bytes: ${ex.getMessage}"
          )

      conn.registerInputHandler()

  def getInfoEntries(role: Role): Seq[(String, String)] =
    role match
      case Master(replID, replOffset, _) =>
        Seq(
          "role" -> "master",
          "master_replid" -> replID,
          "master_repl_offset" -> replOffset.toString
        )
      case Slave(_, _) =>
        Seq(
          "role" -> "slave"
        )
