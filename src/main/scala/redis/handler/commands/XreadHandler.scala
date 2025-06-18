package redis.handler.commands

import redis.Entry
import redis.EntryID
import redis.StreamStore
import redis.formats.RESPData
import redis.handler.Handler

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** XREAD command to read entries from one or more streams. The EntryID for a
  * stream would be None if tehe $ ID is used to get only the new entries. The
  * block parameter is used to specify the timeout for blocking reads.
  *
  * @param streams
  * @param block
  */
case class XreadCommand(
    streams: List[(String, EntryID)],
    block: Option[Int]
)

object XreadCommand:
  private def parseBlockOption(
      args: Array[String]
  ): Try[(Array[String], Option[Int])] =
    if !args.isEmpty && args.head.toUpperCase == "BLOCK"
    then
      if args.length < 2 then
        Failure(Exception("BLOCK option requires a timeout value"))
      else Success(args.drop(2) -> Some(args(1).toInt))
    else Success(args -> None)

  private def parseStreams(
      args: Array[String]
  ): Try[List[(String, Option[EntryID])]] =
    if args.length < 3 || args(0).toUpperCase != "STREAMS" then
      Failure(
        Exception(
          "XREAD command requires 'STREAMS' keyword followed by stream names"
        )
      )
    else
      val streamArgs = args.drop(1)
      if streamArgs.length % 2 != 0 then
        Failure(
          Exception(
            "XREAD command requires an even number of arguments after 'STREAMS'"
          )
        )
      else
        val len = streamArgs.length
        Success((for {
          i <- 0 until len / 2
          streamName = streamArgs(i)
          entryID = streamArgs(i + len / 2) match
            case "$" => None // Use None for $ ID
            case id  => Some(EntryID.forRange(id))
        } yield (streamName, entryID)).toList)

  def apply(args: Array[String]): Try[XreadCommand] =
    parseBlockOption(args.drop(1)).flatMap { case (remainingArgs, block) =>
      parseStreams(remainingArgs).flatMap { streams =>
        // If any ID is "$", then replace it with the maximum EntryID
        // Also check that the blocking option is specified in such a case
        val updatedStreams = streams.map {
          case (streamName, Some(id)) => (streamName, id)
          case (streamName, None) =>
            if block.isEmpty then
              throw new IllegalArgumentException(
                "Blocking read requires a timeout value"
              )
            else
              (
                streamName,
                StreamStore.lastEntry(streamName).getOrElse(EntryID(0, 0))
              )
        }
        Success(XreadCommand(updatedStreams, block))
      }
    }

object XreadHandler extends Handler:
  type StreamResult = (String, List[Entry])

  def handle(args: Array[String]): Try[RESPData] =
    XreadCommand(args)
      .map(
        getStreamResultWithBlocking(_)
      )
      .map(streams =>
        RESPData.Array(
          streams.map((streamName, entries) =>
            RESPData.Array(
              RESPData.BulkString(streamName),
              RESPData.Array(entries.map(_.getResp))
            )
          )
        )
      )

  /** Retrieves entries from the specified stream that have an ID greater than
    * the provided entryID. If the stream does not exist, returns an empty list.
    * @param streamName
    * @param entryID
    */
  private def getStreamEntries(
      streamName: String,
      entryID: EntryID
  ): List[Entry] =
    StreamStore.getStream(streamName) match
      case Some(entries) => entries.filter(_.id > entryID)
      case None          => List.empty[Entry]

  private def getStreamResult(
      streams: List[(String, EntryID)]
  ): List[StreamResult] =
    streams.map((streamName, entryID) =>
      (streamName, getStreamEntries(streamName, entryID))
    )

  /** Checks if there are any new entries in the stream compared to the last
    * saved entry ID.
    * @param streamName
    * @param lastSavedEntryID
    * @return
    */
  private def hasNewEntry(
      streamName: String,
      lastSavedEntryID: Option[EntryID]
  ): Boolean =
    StreamStore.lastEntry(streamName) match
      case Some(lastEntry) =>
        lastSavedEntryID match
          case Some(id) => lastEntry > id
          case None =>
            true // If no last saved entry, then there are new entries
      case None => false

  @tailrec
  private def getNewStreamEntries(
      streamNames: List[String],
      lastSavedEntries: List[Option[EntryID]]
  ): List[StreamResult] =
    val allHaveNewEntries = streamNames
      .zip(lastSavedEntries)
      .forall((streamName, lastSavedEntry) =>
        hasNewEntry(streamName, lastSavedEntry)
      )
    if allHaveNewEntries then
      // If there are new entries, get them
      val streams =
        streamNames.zip(lastSavedEntries.map(_.getOrElse(EntryID(0, 0))))
      getStreamResult(streams)
    else
      // Block for a same time and then check again
      Thread.sleep(50) // 50 milliseconds
      getNewStreamEntries(streamNames, lastSavedEntries)

  private def getStreamResultWithBlocking(
      args: XreadCommand
  ): List[StreamResult] =
    args.block match
      case None    => getStreamResult(args.streams)
      case Some(0) =>
        // Get the last saved entry IDs for each stream and then
        // fetch only the new entries
        val streamNames = args.streams.map(_._1)
        val lastSavedEntryIDs = streamNames.map(StreamStore.lastEntry)
        getNewStreamEntries(streamNames, lastSavedEntryIDs)
      case Some(timeout) =>
        Thread.sleep(timeout)
        getStreamResult(args.streams)
