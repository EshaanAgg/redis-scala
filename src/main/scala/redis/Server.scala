package redis

import java.net.InetSocketAddress
import java.net.ServerSocket
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import handler.Handler

object Server:
  @main def main(): Unit =
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress("localhost", 6379))
    println("Server started at port: 6379")

    while (true) {
      val clientSocket = serverSocket.accept()
      println(s"Accepted connection from: ${clientSocket.getLocalAddress}")
      Future(Handler.socketHandler(clientSocket))
    }

end Server
