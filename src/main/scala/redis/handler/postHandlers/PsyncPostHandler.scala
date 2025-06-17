package redis.handler.postHandlers

import _root_.redis.Connection
import _root_.redis.handler.PostMessageHandler

object PsyncPostHandler extends PostMessageHandler:
  val EMPTY_RDB_FILE_HEX =
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

  val EMPTY_RDB_FILE: Array[Byte] = EMPTY_RDB_FILE_HEX
    .grouped(2)
    .map(Integer.parseInt(_, 16).toByte)
    .toArray

  def handle(_args: Array[String], conn: Connection): Unit =
    // Send empty RBD file to the replica
    // $<length>\r\n<content>
    val length = EMPTY_RDB_FILE.length
    val bytesArr = Array.concat(
      s"$$$length\r\n".getBytes("UTF-8"),
      EMPTY_RDB_FILE
    )
    conn.sendBytes(bytesArr)
