package redis.handler

import redis.RESP2.DataType

import java.io.InputStream
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ParserException(message: String, cause: Throwable = null)
    extends Exception(message, cause)

object Parser:
  def getCommand(in: InputStream): Try[Array[String]] =
    DataType(in) match
      case Failure(err) =>
        throw new ParserException("Failed to serialize command", err)
      case Success(v) =>
        println(s"Received command: ${v.toString}")
        v match
          case DataType.Array(arr) =>
            arr match
              case None =>
                throw new ParserException("Received empty array as command")
              case Some(arr) =>
                Success(
                  arr.map {
                    case DataType.BulkString(str) =>
                      str match
                        case None    => ""
                        case Some(s) => s

                    case DataType.SimpleString(str) => str
                    case x =>
                      throw new ParserException(
                        s"Expected all the array values to be decodable to string, but received value $x"
                      )
                  }.toArray
                )
          case x =>
            throw new ParserException(
              s"Expected to receive a non-null array of arguments, instead gto $x"
            )
