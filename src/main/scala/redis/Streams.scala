package redis

import scala.collection.mutable.TreeMap
import redis.formats.RESPData
import redis.formats.RESPData.BulkString
import scala.util.Try
import scala.util.Failure
import scala.util.Success

case class EntryID(msTime: Long, seq: Long):
    override def toString: String = s"$msTime-$seq"

    def getRESP: RESPData =
        BulkString(this.toString)

object EntryID:
    def apply(id: String): Try[EntryID] =
        val parts = id.split("-")
        if parts.length != 2 
            then Failure(Exception(s"Invalid EntryID format: $id"))
            else Success(EntryID(parts(0).toLong, parts(1).toLong))

case class Entry(id: EntryID, fields: Map[String, String])

object Entry:
    def apply(args: Array[String]): Try[Entry] =
        EntryID(args(0)).map(id =>
            val fields = args.drop(1).grouped(2).map(part => part(0) -> part(1)).toMap
            Entry(id, fields)
        )

type StreamData = List[Entry]

object StreamStore:
    val streams = TreeMap[String, StreamData]()

    /**
      * Creates a new stream with the given name if it does not already exist.
      * Throws an exception if the stream already exists.
      * @param name The name of the stream to create.
      */
    def makeStream(name: String): Unit =
        if !streams.contains(name) then
            streams(name) = List.empty
        else
            throw Exception(s"Stream '$name' already exists")
    
    /**
      * Adds an entry to the specified stream.
      * Creates the stream if it does not exist.
      *
      * @param name The name of the stream to which the entry will be added.
      * @param entry The entry to add to the stream.
      */
    def addEntryToStream(name: String, entry: Entry): Unit =
        if streams.contains(name) then
            val stream = streams(name)
            streams(name) = stream :+ entry
            Some(entry)
        else
            makeStream(name)
            addEntryToStream(name, entry)
    
    def streamExists(name: String): Boolean =
        streams.contains(name)

