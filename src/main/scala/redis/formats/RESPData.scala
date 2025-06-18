package redis.formats

import redis.utils.Common

import java.io.InputStream
import java.nio.charset.StandardCharsets
import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait RESPData:
  def encode: String

  final def getBytes: Array[Byte] =
    encode.getBytes

object RESPData:
  // Define reading methods on the decoder instance specific
  // to the RESP data format
  extension (d: Decoder)
    private def readString: Try[String] =
      d.readToNextCRLF
        .map(String(_, StandardCharsets.UTF_8))

    private def readInteger: Try[Int] =
      readString.map(_.toInt)

  def apply(in: InputStream): Try[RESPData] =
    RESPData(Decoder(in))

  def apply(d: Decoder): Try[RESPData] =
    d.peekByte match
      case Some('+') => SimpleString(d)
      case Some('$') => BulkString(d)
      case Some('-') => Error(d)
      case Some(':') => Integer(d)
      case Some('*') => Array(d)
      case Some('#') => Boolean(d)
      case Some(x) =>
        Failure(DecoderException(s"Unrecognized RESP type marker '$x'"))
      case None =>
        Failure(
          DecoderException(
            "Unexpected end of stream while reading RESP data"
          )
        )

  case class SimpleString(str: String) extends RESPData:
    def encode: String = s"+$str\r\n"
    override def toString: String = s"Simple('$str')"

  case class BulkString(str: Option[String]) extends RESPData:
    def encode: String =
      str match
        case Some(str) => s"$$${str.length}\r\n$str\r\n"
        case None      => "$-1\r\n"

    override def toString: String = str match
      case Some(str) => s"Bulk('$str')"
      case None      => "Bulk(NULL)"

  case class Error(msg: String) extends RESPData:
    def encode: String = s"-$msg\r\n"
    override def toString: String = s"Err('$msg')"

  case class Integer(v: Int) extends RESPData:
    def encode: String = s":$v\r\n"
    override def toString: String = s"Int($v)"

  case class Array(arr: Option[List[RESPData]]) extends RESPData:
    def encode: String =
      arr match
        case Some(arr) =>
          val arrVals = arr.map(_.encode)
          arrVals.mkString(s"*${arr.length}\r\n", "", "")
        case None => "*-1\r\n"

    override def toString: String = arr match
      case Some(arr) =>
        s"Arr${arr.map(_.toString).mkString("(", ", ", ")")}"
      case None => "Arr(NULL)"

  case class Boolean(b: scala.Boolean) extends RESPData:
    def encode: String = s"#${if b then 't' else 'f'}\r\n"
    override def toString: String = s"Bool(${if b then "T" else "F"})"

  object SimpleString:
    def apply(d: Decoder): Try[SimpleString] =
      d.expectByte('+').flatMap(_ => d.readString.map(SimpleString(_)))

  object BulkString:
    object Null extends BulkString(None)

    def apply(d: Decoder): Try[BulkString] =
      d.expectByte('$')
        .flatMap(_ =>
          d.readInteger.flatMap(l =>
            if l == -1
            then Success(BulkString(None))
            else
              d.readString.flatMap(s =>
                if s.length == l
                then Success(BulkString(s))
                else
                  Failure(
                    DecoderException(
                      s"Length of read bytes is ${s.length} while expected it to be $l"
                    )
                  )
              )
          )
        )

    def apply(str: String): BulkString = BulkString(Some(str))

  object Error:
    def apply(d: Decoder): Try[Error] =
      d.expectByte('-').flatMap(_ => d.readString.map(Error(_)))

  object Integer:
    def apply(d: Decoder): Try[Integer] =
      d.expectByte(':').flatMap(_ => d.readInteger.map(Integer(_)))

  object Array:
    def apply(d: Decoder): Try[Array] =
      d.expectByte('*')
        .flatMap(_ =>
          d.readInteger.flatMap(l =>
            if l == -1
            then Success(Array(None))
            else
              val elements = (1 to l).map(_ => RESPData(d))
              Common
                .sequenceTries(elements)
                .flatMap(arr =>
                  if arr.length == l
                  then Success(Array(arr))
                  else
                    Failure(
                      DecoderException(
                        s"Length of elements parsed is ${arr.length} while expected it to be $l"
                      )
                    )
                )
          )
        )

    def apply(arr: RESPData*): Array = Array(Some(arr.toList))
    def apply(arr: List[RESPData]): Array = Array(Some(arr))

  object Boolean:
    val True: Boolean = Boolean(true)
    val False: Boolean = Boolean(false)

    def apply(d: Decoder): Try[Boolean] =
      d.expectByte('#')
        .flatMap(_ =>
          d.readString.flatMap {
            case "f" => Success(False)
            case "t" => Success(True)
            case x =>
              Failure(
                DecoderException(
                  s"Unexpected value '$x' for Boolean type"
                )
              )
          }
        )
