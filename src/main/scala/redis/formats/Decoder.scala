package redis.formats

import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.InputStream
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.boundary

import boundary.break

class DecoderException(message: String, cause: Throwable = null)
    extends Exception(message, cause)

case class Decoder(
  rawIn: InputStream,
  readBytes: ArrayBuffer[Byte] = ArrayBuffer.empty
):
  private val in: BufferedInputStream =
    rawIn match
      case b: BufferedInputStream => b
      case _                      => new BufferedInputStream(rawIn)

  def readNBytes(n: Int): Try[Array[Byte]] =
    val arr = in.readNBytes(n)
    readBytes ++= arr
    if arr.length != n then
      Failure(Exception(s"Expected $n bytes, got ${arr.length}"))
    else Success(arr)

  def readByte: Try[Byte] =
    val value = in.read()
    if value == -1 then Failure(Exception("Stream ended unexpectedly"))
    else
      readBytes.append(value.toByte)
      Success(value.toByte)

  def peekByte: Option[Byte] =
    in.mark(1)
    val byte = in.read()
    in.reset()
    if byte == -1 then None else Some(byte.toByte)

  def expectByte(expected: Byte): Try[Unit] =
    readByte.flatMap { b =>
      if b == expected then Success(())
      else
        Failure(
          new DecoderException(
            s"Expected byte $expected, but got $b"
          )
        )
    }

  def readToNextCRLF: Try[Array[Byte]] = Try {
    val result = ArrayBuffer[Byte]()
    var foundCR = false

    boundary:
      while true do
        readByte match
          case Success(next) =>
            next match
              case '\r' => foundCR = true
              case '\n' =>
                if foundCR then break()
                else result.append(next)
              case _ =>
                if foundCR then
                  foundCR = false
                  result.append('\r'.toByte)
                result.append(next)
          case Failure(e) => throw e

    result.toArray
  }

  def isEndOfStream: Boolean =
    peekByte.isEmpty

object Decoder:
  def apply(bytes: Array[Byte]): Decoder =
    new Decoder(new ByteArrayInputStream(bytes))
