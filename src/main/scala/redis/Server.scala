package redis

import java.net.InetSocketAddress
import java.net.ServerSocket
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import handler.Handler

object ArgsParser:
  private type OptionMap = Map[String, String]

  private val recognizedOptions = Set("--dir", "--dbfile", "--port", "--replicaof")

  def parse(args: Seq[String]): OptionMap =
    @tailrec
    def getNextOption(map: OptionMap, args: List[String]): OptionMap =
      args match
        case Nil => map
        case key :: value :: next if recognizedOptions.contains(key) =>
          getNextOption(map ++ Map(key.stripPrefix("--") -> value), next)
        case args => throw Exception(s"Malformed arguments: $args")

    getNextOption(Map(), args.toList)

@main def main(args: String*): Unit =
  ServerState.updateStateFromCLIArgs(
    ArgsParser.parse(args)
  )

  val serverSocket = new ServerSocket()
  serverSocket.bind(new InetSocketAddress("localhost", ServerState.port))
  println(s"Server started at port: ${ServerState.port}")

  while (true) {
    val clientSocket = serverSocket.accept()
    println(s"Accepted connection from :${clientSocket.getPort}")
    Future(Handler.socketHandler(clientSocket))
  }
