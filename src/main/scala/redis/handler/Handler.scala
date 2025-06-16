package redis.handler

import redis.formats.RESPData
import redis.handler.commands.ConfigHandler
import redis.handler.commands.EchoHandler
import redis.handler.commands.GetHandler
import redis.handler.commands.KeysHandler
import redis.handler.commands.PingHandler
import redis.handler.commands.SetHandler
import redis.handler.commands.UnknownHandler

import java.io.InputStream
import java.net.Socket
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Handler:
  def handle(args: Array[String]): Try[RESPData]

object Handler:
  private def process(in: InputStream): Try[Array[Byte]] =
    Parser
      .getCommand(in)
      .flatMap(cmd =>
        val h = cmd(0).toLowerCase() match
          case "ping"   => PingHandler
          case "echo"   => EchoHandler
          case "set"    => SetHandler
          case "get"    => GetHandler
          case "config" => ConfigHandler
          case "keys"   => KeysHandler
          case _        => UnknownHandler

        val resp = h.handle(cmd)
        println(s"Sending response: ${resp.toString}")
        resp.map(_.getBytes)
      )

  def socketHandler(socket: Socket): Unit =
    val in = socket.getInputStream
    val out = socket.getOutputStream

    while (!socket.isClosed) {
      process(in) match
        case Failure(err) =>
          val errorMessage = RESPData.Error(err.toString)
          out.write(errorMessage.getBytes)
        case Success(bytes) =>
          out.write(bytes)
    }
