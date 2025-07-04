package redis

import redis.Role.Master
import redis.Role.Slave
import redis.formats.Decoder
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import redis.formats.RESPData.NoResponse
import redis.handler.Handler

import java.io.OutputStream
import java.net.Socket
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Connection(
  host: String,
  port: Int,
  conn: Socket,
  isMasterConnection: Boolean
):
  val d: Decoder = Decoder(
    conn.getInputStream
  ) // The decoder for the input stream
  val out: OutputStream = conn.getOutputStream

  val logPrefix: String =
    s"[${if isMasterConnection then "M" else "C"} :${port}]"
  var inTransaction: Boolean = false
  val queuedCommands: ArrayBuffer[Array[String]] = ArrayBuffer()

  def disconnect(): Unit =
    conn.close()
    // Remove the connection from replicas for Master role
    ServerState.role match
      case m: Master => m.removeReplica(this)
      case _: Slave  => ()

  /** Sends a byte array to the connection's output stream.
    * @param bytes
    *   The byte array to be sent.
    */
  def sendBytes(bytes: Array[Byte]): Unit =
    Try {
      out.write(bytes)
      out.flush()
    } recover { case e: Exception =>
      println(
        s"${logPrefix} Error sending bytes ${bytes.take(10).mkString("[", ", ", "]")}...: ${e.getMessage}"
      )
      disconnect()
    }

  def sendAndExpectResponse(toSend: RESPData, expect: RESPData): Unit =
    sendBytes(toSend.getBytes)
    RESPData(d) match
      case Success(v) =>
        if v != expect then
          throw new Exception(
            s"Sent data ($toSend), but expected response $expect, got $v"
          )
      case Failure(ex) =>
        throw new Exception(
          s"Sent data ($toSend), but failed to read response: ${ex.getMessage}"
        )

  /** Sends the given RESPData to the output stream and tries to read a response
    * from the input stream. If the response is successfully read, it returns
    * the response wrapped in a Try. If there is an error reading the response,
    * it returns a Failure with the error.
    */
  def sendAndTryResponse(
    toSend: RESPData
  ): Try[RESPData] =
    sendBytes(toSend.getBytes)
    RESPData(d)

  /** Sends the given RESPData to the output stream and waits for a response. If
    * the response is successfully read, it returns the response. If there is an
    * error reading the response, it throws an Exception with the error message.
    */
  def sendAndGetResponse(toSend: RESPData): RESPData =
    sendAndTryResponse(toSend) match
      case Success(resp) => resp
      case Failure(ex) =>
        throw new Exception(
          s"Failed to read response after sending '$toSend': ${ex.getMessage}"
        )

  /** Checks if there is data available in the input stream. Makes use of the
    * decoder to peek the next byte, as the same buffers the stream used.
    * @return
    *   true if there is data available, false otherwise.
    */
  def hasData: Boolean = !conn.isClosed || d.peekByte.isDefined

  def sendData(data: RESPData): Unit =
    data match
      case NoResponse => ()
      case d          => sendBytes(d.getBytes)

  // Should send the results back to the client
  // only if it not a master connection, or it is a REPLCONF GETACK command
  // from the master
  def shouldSendCommandResult(cmd: Array[String]): Boolean =
    if !isMasterConnection then true
    else
      cmd.length >= 2 && cmd(0).toLowerCase == "replconf" && cmd(
        1
      ).toLowerCase == "getack"

  def registerInputHandler(): Unit =
    new Thread(() =>
      try while hasData do Handler.connectionHandler(d, this)
      catch
        case e: Exception =>
          println(s"${logPrefix} Unexpected error: ${e.getMessage}")
          disconnect()).start()

  /** Updates the acknowledged offset for the connection. This is only done for
    * the master connection, and is used to track the last acknowledged offset
    * by the replica. This should be called after the command has been
    * processed, so that the current command is not included in the acknowledged
    * offset.
    *
    * @param args
    */
  def updateAcknowledgedOffset(args: Array[String]): Unit =
    if isMasterConnection then
      ServerState.role match
        case s: Slave =>
          val cmd = RESPData.Array(args.map(RESPData.BulkString(_)).toList)
          s.acknowledgedOffset += cmd.getBytes.length
        case _: Master =>
          throw new IllegalStateException(
            "isMasterConnection is set true for a Master role, which is not allowed."
          )

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
