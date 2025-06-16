package redis

import java.net.InetSocketAddress
import java.net.ServerSocket
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import handler.Handler

object ArgsParser:
  private type OptionMap = Map[String, Any]

  def parse(args: Seq[String]): OptionMap =
    @tailrec
    def getNextOption(map: OptionMap, args: List[String]): OptionMap =
      args match
        case Nil => map
        case "--dir" :: value :: next =>
          getNextOption(map ++ Map("dir" -> value), next)
        case "--dbfilename" :: value :: next =>
          getNextOption(map ++ Map("dbfile" -> value), next)
        case args => throw Exception(s"Malformed arguments: $args")

    getNextOption(Map(), args.toList)

@main def main(args: String*): Unit =
  ServerState.updateStateFromCLIArgs(
    ArgsParser.parse(args)
  )

  val serverSocket = new ServerSocket()
  serverSocket.bind(new InetSocketAddress("localhost", 6379))
  println("Server started at port: 6379")

  while (true) {
    val clientSocket = serverSocket.accept()
    println(s"Accepted connection from: ${clientSocket.getLocalAddress}")
    Future(Handler.socketHandler(clientSocket))
  }
