package redis

import redis.formats.RDBFile
import redis.formats.RESPData

import java.time.Instant
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.util.Try

case class StoreVal(data: RESPData, exp: Option[Instant]):
  def isDefined: Boolean =
    exp.isEmpty || exp.get.isAfter(Instant.now())

  def isEmpty: Boolean = !isDefined

object ServerState:
  private val store: TrieMap[String, StoreVal] = new TrieMap()
  var dir: String = "./sample"
  var dbFile: String = "dump.rdb"
  var port: Int = 6379
  var role: Role = Role.Master(
    UUID.randomUUID().toString.replace("-", ""),
    0L,
    ListBuffer.empty[Connection]
  )

  /** Returns all the replicas that the connected server should stream data to.
    * If the server is a slave, it returns an empty array.
    * @return
    *   Array of connections to the replicas
    */
  def replicas: Array[Connection] =
    role match
      case Role.Master(_, _, replicas) => replicas.toArray
      case Role.Slave(_, _)            => Array.empty[Connection]

  /** Updates the server state at startup from the map of options provided by
    * the user
    * @param args
    *   Map of key-value pairs for various configurations
    */
  def updateStateFromCLIArgs(args: Map[String, String]): Unit =
    args.foreach((k, v) =>
      k match
        case "dir"        => dir = v
        case "dbfilename" => dbFile = v
        case "port"       => port = v.toInt
        case "replicaof"  =>
          // Expecting format: "host port"
          val parts = v.split(" ")
          if parts.length == 2 && parts(0) == "localhost" then
            role = Role.Slave(
              parts(0),
              parts(1).toInt
            )
          else println(s"Invalid replicaof format: $v")

        case _ => println(s"Unrecognized key-value pair: $k -> $v")
    )

    val rdbFileResult = RDBFile.loadFile(s"$dir/$dbFile")
    if rdbFileResult.isDefined then
      println(s"Error loading RDB file: ${rdbFileResult.get}")

  def performHandshake: Unit =
    val handshake = Try {
      role.performHandshake
    }
    if handshake.isFailure then
      println(s"[Handshake] Failed: ${handshake.failed.get.getMessage}")
    else println("[Handshake] Completed successfully")

  /** Adds a new member to persistent storage
    * @param k
    *   The key associated with the item
    * @param v
    *   The value of the pair
    */
  def addKey(k: String, v: StoreVal): TrieMap[String, StoreVal] =
    store += k -> v

  /** Returns the value stored in the persistent storage for the provided key.
    * Deletes the same as well if it is expired.
    * @param k
    *   The key to fetch
    * @return
    */
  def get(k: String): Option[StoreVal] =
    store
      .get(k)
      .flatMap(v =>
        if v.isEmpty then
          store -= k
          None
        else Some(v)
      )

  /** Returns all the keys stored in the database currently. Filters out the
    * expired keys.
    */
  def keys: Seq[String] =
    store
      .filter(_._2.isDefined)
      .keys
      .toSeq
