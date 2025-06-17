package redis

import redis.formats.RESPData

import java.io.InputStream
import java.net.Socket
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Connection(
    val host: String,
    val port: Int,
    val conn: Socket,
    val isMasterConnection: Boolean
):
  def disconnect: Unit =
    conn.close()

  /** Sends a byte array to the connection's output stream.
    * @param bytes
    *   The byte array to be sent.
    */
  def sendBytes(bytes: Array[Byte]): Unit =
    Try {
      val out = conn.getOutputStream
      out.write(bytes)
      out.flush()
    } recover { case e: Exception =>
      println(s"Error sending bytes: ${e.getMessage}")
      disconnect
    }

  def sendAndExpectResponse(toSend: RESPData, expect: RESPData): Unit =
    sendBytes(toSend.getBytes)
    val in = conn.getInputStream
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
    val in = conn.getInputStream
    RESPData(in) match
      case Failure(ex) =>
        throw new Exception(
          s"Failed to read response for $toSend: ${ex.getMessage}"
        )
      case Success(v) => v

  def isClosed: Boolean = conn.isClosed

  def inputStream: InputStream =
    conn.getInputStream

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
    Connection(socket.getInetAddress.getHostName, socket.getPort, socket, false)

  def apply(
      host: String,
      port: Int,
      isMasterConnection: Boolean = false
  ): Connection =
    val socket = new Socket(host, port)
    Connection(host, port, socket, isMasterConnection)
