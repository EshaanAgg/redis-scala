package redis.formats

import java.io.InputStream
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.boundary

import boundary.break

class DecoderException(message: String, cause: Throwable = null)
    extends Exception(message, cause)

case class Decoder(in: InputStream):
  def readByte: Try[Byte] =
    val arr = in.readNBytes(1)
    if arr.isEmpty then throw new Exception("Stream ended unexpectedly")
    Success(arr.head)

  def peekByte: Option[Int] =
    in.mark(1)
    val peekedByte = Try {
      Some(in.read)
    }.getOrElse(None)

    in.reset() // Reset the stream
    peekedByte

  def readNBytes(n: Int): Try[Array[Byte]] =
    val arr = in.readNBytes(n)
    if arr.isEmpty then
      throw new Exception(s"Expected to read $n bytes, but the stream ended")
    Success(arr)

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

end Decoder
