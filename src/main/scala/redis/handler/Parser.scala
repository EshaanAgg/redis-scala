package redis.handler

import redis.formats.Decoder
import redis.formats.RESPData

import java.io.IOException
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ParserException(message: String, cause: Throwable = null)
    extends Exception(message, cause):

  override def getMessage: String =
    if cause == null then message
    else s"$message: ${cause.getMessage}"

object Parser:
  def getCommand(d: Decoder): Try[Array[String]] =
    RESPData(d) match
      case Failure(err) =>
        if err.isInstanceOf[IOException] then Failure(err)
        else Failure(new ParserException("Failed to parse RESP data", err))
      case Success(v) =>
        v match
          case RESPData.Array(arr) =>
            arr match
              case None =>
                throw new ParserException(
                  "Received empty array as command"
                )
              case Some(arr) =>
                Success(
                  arr.map {
                    case RESPData.BulkString(str) =>
                      str match
                        case None    => ""
                        case Some(s) => s

                    case RESPData.SimpleString(str) => str
                    case x =>
                      throw new ParserException(
                        s"Expected all the array values to be decodable to string, but received value $x"
                      )
                  }.toArray
                )
          case x =>
            throw new ParserException(
              s"Expected to receive a non-null array of arguments, instead got $x"
            )
