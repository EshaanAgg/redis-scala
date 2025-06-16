package redis

import redis.formats.RDBFile
import redis.formats.RESPData

import java.time.Instant
import scala.collection.concurrent.TrieMap

case class StoreVal(data: RESPData, exp: Option[Instant]):
  private def isDefined: Boolean =
    exp.isEmpty || exp.get.isBefore(Instant.now())
  def isEmpty: Boolean = !isDefined

object ServerState:
  private val store: TrieMap[String, StoreVal] = new TrieMap()
  var dir: String = "./sample"
  var dbFile: String = "dump.rdb"

  /** Updates the server state at startup from the map of options provided by
    * the user
    * @param args
    *   Map of key-value pairs for various configurations
    */
  def updateStateFromCLIArgs(args: Map[String, Any]): Unit =
    args.foreach((k, v) =>
      k match
        case "dir"    => dir = v.toString
        case "dbfile" => dbFile = v.toString
        case _        => println(s"Unrecognized key-value pair: $k -> $v")
    )

    val rdbFileResult = RDBFile.loadFile(s"$dir/$dbFile")
    if rdbFileResult.isDefined then
      println(s"Error loading RDB file: ${rdbFileResult.get}")

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
      .filter((_, v) => v.exp.isEmpty || v.exp.get.isBefore(Instant.now()))
      .keys
      .toSeq
