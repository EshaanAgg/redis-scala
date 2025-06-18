package redis

import redis.formats.RESPData
import redis.handler.Handler

import java.net.Socket
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Connection(
    val host: String,
    val port: Int,
    val conn: Socket,
    val isMasterConnection: Boolean
):
  val in = conn.getInputStream
  val out = conn.getOutputStream

  var inTransaction: Boolean = false
  val queuedCommands: ArrayBuffer[Array[String]] = ArrayBuffer()

  def disconnect: Unit =
    conn.close()

  /** Read incoming bytes from the connection.
    */
  def getBytes: Array[Byte] =
    Try {
      val buffer = new Array[Byte](1024)
      val bytesRead = in.read(buffer)
      if bytesRead == -1 then Array.emptyByteArray
      else buffer.take(bytesRead)
    } recover { case e: Exception =>
      println(s"Error reading bytes: ${e.getMessage}")
      disconnect
      Array.emptyByteArray
    } getOrElse Array.emptyByteArray

  /** Sends a byte array to the connection's output stream.
    * @param bytes
    *   The byte array to be sent.
    */
  def sendBytes(bytes: Array[Byte]): Unit =
    Try {
      out.write(bytes)
      out.flush()
    } recover { case e: Exception =>
      println(s"Error sending bytes: ${e.getMessage}")
      disconnect
    }

  def sendAndExpectResponse(toSend: RESPData, expect: RESPData): Unit =
    sendBytes(toSend.getBytes)
    RESPData(in) match
      case Success(v) =>
        if v != expect then
          throw new Exception(
            s"Sent data ($toSend), but expected response $expect, got $v"
          )
      case Failure(ex) =>
        throw new Exception(
          s"Sent data ($toSend), but failed to read response: ${ex.getMessage}"
        )

  def sendAndGetResponse(toSend: RESPData): RESPData =
    sendBytes(toSend.getBytes)
    RESPData(in) match
      case Failure(ex) =>
        throw new Exception(
          s"Failed to read response for $toSend: ${ex.getMessage}"
        )
      case Success(v) => v

  def isClosed: Boolean = conn.isClosed

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

  def registerInputHandler: Unit =
    new Thread(() =>
      try while !isClosed do Handler.connectionHandler(in, this)
      catch
        case e: Exception =>
          println(s"[:${port}] Unexpected error: ${e.getMessage}")
          disconnect
    ).start()

object Connection:
  def apply(socket: Socket): Connection =
    Connection(
      socket.getInetAddress.getHostName,
      socket.getPort,
      socket,
      false
    )

  def apply(
      host: String,
      port: Int,
      isMasterConnection: Boolean = false
  ): Connection =
    val socket = new Socket(host, port)
    Connection(host, port, socket, isMasterConnection)
