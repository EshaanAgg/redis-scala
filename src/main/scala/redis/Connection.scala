package redis

import redis.formats.RESPData

import java.io.InputStream
import java.net.Socket
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class Connection(val host: String, val port: Int):
  private var conn: Option[Socket] = None
  var isMasterConnection = true

  def connection: Socket =
    if conn.isEmpty then conn = Some(new Socket(host, port))
    conn.get

  def disconnect: Unit =
    if conn.isDefined then
      conn.get.close()
      conn = None

  /** Sends a byte array to the connection's output stream.
    * @param bytes
    *   The byte array to be sent.
    */
  def sendBytes(bytes: Array[Byte]): Unit =
    Try {
      val out = connection.getOutputStream
      out.write(bytes)
      out.flush()
    } recover { case e: Exception =>
      println(s"Error sending bytes: ${e.getMessage}")
      disconnect
    }

  def sendAndExpectResponse(toSend: RESPData, expect: RESPData): Unit =
    sendBytes(toSend.getBytes)
    val in = connection.getInputStream
    RESPData(in) match
      case Success(v) =>
        if v != expect then
          throw new Exception(
            s"Sent data ($toSend), but expected response $expect, got $v"
          )
      case Failure(ex) =>
        throw new Exception(
          s"Sent data ($toString), but failed to read response: ${ex.getMessage}"
        )

  def sendAndGetResponse(toSend: RESPData): RESPData =
    sendBytes(toSend.getBytes)
    val in = connection.getInputStream
    RESPData(in) match
      case Failure(ex) =>
        throw new Exception(
          s"Failed to read response for $toSend: ${ex.getMessage}"
        )
      case Success(v) => v

  def isClosed: Boolean = conn.isEmpty || conn.get.isClosed

  def inputStream: InputStream =
    if conn.isEmpty then throw new Exception("Connection is not established")
    conn.get.getInputStream

  def sendData(data: RESPData): Unit =
    sendBytes(data.getBytes)

  // Should send the results back to the client
  // only if it not a master connection, or it is a REPLCONF GETACK command
  // from the master
  def shouldSendCommandResult(cmd: Array[String]): Boolean =
    if !isMasterConnection then true
    else
      cmd.length >= 2 && cmd(0).toLowerCase == "replconf" && cmd(
        1
      ).toLowerCase == "getack"

object Connection:
  def apply(socket: Socket): Connection =
    val conn = new Connection(socket.getInetAddress.getHostName, socket.getPort)
    conn.conn = Some(socket)
    conn
