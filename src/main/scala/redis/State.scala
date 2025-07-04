package redis

import redis.ds.LinkedList
import redis.formats.RDBFile

import java.time.Instant
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.util.Try

case class StoreVal(data: String, exp: Option[Instant]):
  def isDefined: Boolean =
    exp.isEmpty || exp.get.isAfter(Instant.now())

  def isEmpty: Boolean = !isDefined

object ServerState:
  private val store: TrieMap[String, StoreVal] = new TrieMap()
  private val listStore: TrieMap[String, LinkedList[String]] = new TrieMap()

  // Mutex for synchronization of the server state between different threads
  // so that the commands are processed in order.
  val mtx = new ReentrantLock()

  var dir: String = "./sample"
  var dbFile: String = "dump.rdb"
  var port: Int = 6379
  var role: Role = Role.Master(
    UUID.randomUUID().toString.replace("-", ""),
    0L,
    ListBuffer.empty[Replica]
  )

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

  def performHandshake(): Unit =
    val handshake = Try {
      role.performHandshake()
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

  /** Increments the value of the key by 1 if it is an integer. If the key does
    * not exist, it initializes it to 1. Returns an error otherwise.
    *
    * @param k
    *   The key to increment
    * @return
    *   Either[String, Int] - Returns the new value if successful, or an error
    *   message
    */
  def incr(k: String): Either[String, Int] =
    store.get(k) match
      // If the key exists and is an integer, increment it by 1
      case Some(StoreVal(v, _)) if v.forall(_.isDigit) =>
        val newValue = v.toInt + 1
        store.update(k, StoreVal(newValue.toString, None))
        Right(newValue)
      // If the key does not exist, create it with value "1"
      case None =>
        addKey(k, StoreVal("1", None))
        Right(1)
      case _ => Left("ERR value is not an integer or out of range")

  /** Returns all the keys stored in the database currently. Filters out the
    * expired keys.
    */
  def keys: Seq[String] =
    store
      .filter(_._2.isDefined)
      .keys
      .toSeq

  /** Returns the linked list associated with the given key, creating it if it
    * does not exist.
    *
    * @param k
    *   The key for the linked list
    * @return
    *   The linked list associated with the key
    */
  def getOrCreateList(k: String): LinkedList[String] =
    listStore.getOrElseUpdate(k, LinkedList[String]())

  /** Checks if the list associated with the given key is non-empty.
    *
    * @param k
    *   The key for the linked list
    * @return
    *   Boolean indicating whether the list is non-empty (if it exists).
    */
  def hasNonEmptyList(k: String): Boolean =
    listStore.get(k) match
      case Some(list) => list.nonEmpty
      case None       => false
