package redis.RESP2

import redis.utils.Common

import java.io.InputStream
import scala.util.{Failure, Success, Try}

sealed trait DataType:
  def encode: String

  final def getBytes: Array[Byte] =
    encode.getBytes

object DataType:
  def apply(in: InputStream): Try[DataType] =
    val d = Decoder(in)
    DataType(d)

  def apply(d: Decoder): Try[DataType] =
    d.readByte.flatMap {
      case '+' => SimpleString(d)
      case '$' => BulkString(d)
      case '-' => Error(d)
      case ':' => Integer(d)
      case '*' => Array(d)
      case '#' => Boolean(d)
      case x => Failure(DecoderException(s"Unrecognized RESP type marker '$x'"))
    }

  case class SimpleString(str: String) extends DataType:
    def encode: String = s"+$str\r\n"
    override def toString: String = s"Simple('$str')"

  case class BulkString(str: Option[String]) extends DataType:
    def encode: String =
      str match
        case Some(str) => "$" + s"${str.length}\r\n$str\r\n"
        case None      => "$-1\r\n"

    override def toString: String = str match
      case Some(str) => s"Bulk('$str')"
      case None      => "Bulk(NULL)"

  case class Error(msg: String) extends DataType:
    def encode: String = s"-$msg\r\n"
    override def toString: String = s"Err('$msg')"

  case class Integer(v: Int) extends DataType:
    def encode: String = s":$v\r\n"
    override def toString: String = s"Int($v)"

  case class Array(arr: Option[List[DataType]]) extends DataType:
    def encode: String =
      arr match
        case Some(arr) =>
          val arrVals = arr.map(_.encode)
          arrVals.mkString(s"*${arr.length}\r\n", "", "")
        case None => "*-1\r\n"

    override def toString: String = arr match
      case Some(arr) => s"Arr${arr.map(_.toString).mkString("(", ", ", ")")}"
      case None      => "Arr(NULL)"

  case class Boolean(b: scala.Boolean) extends DataType:
    def encode: String = s"#${if b then 't' else 'f'}\r\n"
    override def toString: String = s"Bool(${if b then "T" else "F"})"

  object SimpleString:
    def apply(d: Decoder): Try[SimpleString] =
      d.readString.map(SimpleString(_))

  object BulkString:
    def apply(d: Decoder): Try[BulkString] =
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

    def apply(str: String): BulkString = BulkString(Some(str))

  object Error:
    def apply(d: Decoder): Try[Error] =
      d.readString.map(Error(_))

  object Integer:
    def apply(d: Decoder): Try[Integer] =
      d.readInteger.map(Integer(_))

  object Array:
    def apply(d: Decoder): Try[Array] =
      d.readInteger.flatMap(l =>
        if l == -1
        then Success(Array(None))
        else
          val elements = (1 to l).map(_ => DataType(d))
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

    def apply(arr: List[DataType]): Array = Array(Some(arr))

  object Boolean:
    val True: Boolean = Boolean(true)
    val False: Boolean = Boolean(false)

    def apply(d: Decoder): Try[Boolean] =
      d.readString.flatMap {
        case "f" => Success(False)
        case "t" => Success(True)
        case x =>
          Failure(DecoderException(s"Unexpected value '$x' for Boolean type"))
      }
