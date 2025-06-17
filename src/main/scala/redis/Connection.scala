package redis

import redis.formats.RESPData

import java.net.Socket
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class Connection(val host: String, val port: Int):
  private var conn: Option[Socket] = None

  def connection: Socket =
    if conn.isEmpty then conn = Some(new Socket(host, port))
    conn.get

  def disconnect: Unit =
    if conn.isDefined then
      conn.get.close()
      conn = None

  /*
   * Sends a byte array to the connection's output stream.
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
    val response = RESPData(in)
    if response != expect then
      throw new Exception(s"Expected response: $expect, but got: $response")

  def sendAndGetResponse(toSend: RESPData): RESPData =
    sendBytes(toSend.getBytes)
    val in = connection.getInputStream
    RESPData(in) match
      case Failure(ex) =>
        throw new Exception(s"Error reading response: ${ex.getMessage}")
      case Success(v) => v
