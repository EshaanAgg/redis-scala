package redis.formats

import redis.ServerState
import redis.StoreVal
import redis.formats.RESPData.BulkString
import redis.utils.File

import java.time.Instant
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import redis.utils.Convert

object RDBFile:
  private type Metadata = (String, String)
  private type DBRecord = (String, StoreVal)

  private val MetadateSectionByte: Byte = 0xfa.toByte
  private val DatabaseSectionByte: Byte = 0xfe.toByte
  // Define methods on decoder to read datatypes specific to RDB file
  extension (d: Decoder)
    private def expectByte(expected: Byte): Try[Unit] =
      d.readByte.flatMap { b =>
        if b == expected then Success(())
        else
          Failure(
            new DecoderException(
              s"Expected byte $expected, but got $b"
            )
          )
      }

    /** Reads an integer from the RDB file format. The integer can be encoded in
      * different ways based on the first byte read.
      * @return
      *   A Try containing the integer value if successful, or a Failure if an
      *   error occurs.
      */
    private def readInt: Try[Int] =
      d.readByte.flatMap(b =>
        val identifierBits = (b & 0xc0) >> 6
        val otherBits = (b & 0x3f)

        identifierBits match
          case 0 => Success(otherBits)
          case 1 => d.readByte.map(h => h << 6 | otherBits)
          case 2 =>
            d.readNBytes(4)
              .map(Convert.getBENumber(_).toInt)
          case 3 => Success(b) // Number encoded as a string
      )

    /** Reads a string from the RDB file format. The string can be encoded
      * either as a direct byte sequence or as a reference to another string.
      * @return
      *   A Try containing the string value if successful, or a Failure if an
      *   error occurs.
      */
    private def readString: Try[String] =
      readInt.flatMap(len =>
        if (len & 0xc0) == 0xc0 then
          // Read number encoded as a string
          val bufLen = len match
            case -64 => 1 // 0xC0
            case -63 => 2 // 0xC1
            case -62 => 4 // 0xC2
            case _ =>
              throw new DecoderException(s"Unsupported string length: $len")
          
          d.readNBytes(bufLen)
            .map(Convert.getLENumber(_).toString)
        // Read "len" bytes from the stream as a string
        else d.readNBytes(len).map(String(_, "UTF-8"))
      )

  private def readHeaderSection(d: Decoder): Option[String] =
    val supportedHeader = "REDIS0011"

    d.readNBytes(supportedHeader.length) match
      case Success(byteArr) =>
        if byteArr.sameElements(supportedHeader.getBytes)
        then None
        else
          Some(
            s"Required the header section to be '$supportedHeader', but received '${String(byteArr, "UTF-8")}'"
          )
      case Failure(ex) => Some(ex.toString)

  private def readMetadata(d: Decoder): Try[Vector[Metadata]] =
    def loop(acc: Vector[Metadata]): Try[Vector[Metadata]] =
      d.peekByte match
        case Some(MetadateSectionByte) =>
          d.readByte // Consume the metadata section byte
          // Read the metadata section
          val kvTry = for {
            key <- d.readString
            value <- d.readString
          } yield (key -> value)
          kvTry.flatMap(kv => loop(acc :+ kv))
        case _ => Success(acc)

    loop(Vector.empty)

  private def readDatabaseSection(d: Decoder): Try[Vector[DBRecord]] =
    def readRecord: Try[DBRecord] =
      d.expectByte(0).flatMap { _ =>
        for {
          key <- d.readString
          value <- d.readString
        } yield (key, StoreVal(BulkString(value), None))
      }

    def readRecordWithMSExpiry: Try[DBRecord] =
      d.expectByte(0xfc.toByte).flatMap { _ =>
        d.readNBytes(8)
          .flatMap(expBytes => {
            val exp = Convert.getLENumber(expBytes)
            readRecord.map((key, value) =>
              key -> StoreVal(value.data, Some(Instant.ofEpochMilli(exp)))
            )
          })
      }

    def readRecordWithSecExpiry: Try[DBRecord] =
      d.expectByte(0xFD.toByte).flatMap { _ =>
        d.readNBytes(4)
          .flatMap(expBytes => {
            val exp = Convert.getLENumber(expBytes)
            readRecord.map((key, value) =>
              key -> StoreVal(value.data, Some(Instant.ofEpochSecond(exp)))
            )
          })
      }

    def readHeader: Try[Int] =
      d.expectByte(DatabaseSectionByte).flatMap { _ =>
        d.readInt.flatMap { idx =>
          println(s"[Database Section] Index: $idx")
          d.expectByte(0xFB.toByte).flatMap { _ =>
            for {
              tot <- d.readInt
              exp <- d.readInt
            } yield {
              println(
                s"[Database Section] Total Records: $tot, Expiry Records: $exp"
              )
              tot
            }
          }
        }
      }

    def loop(acc: Vector[DBRecord]): Try[Vector[DBRecord]] =
      d.peekByte match
        case Some(0) =>
          readRecord.flatMap(record => loop(acc :+ record))
        case Some(0xFC) =>
          readRecordWithMSExpiry.flatMap(record => loop(acc :+ record))
        case Some(0xFD) =>
          readRecordWithSecExpiry.flatMap(record => loop(acc :+ record))
        case _ => Success(acc)

    d.peekByte match
      case Some(DatabaseSectionByte) =>
        readHeader.flatMap { _ =>
          loop(Vector.empty)
        }
      case _ => Success(Vector.empty) // No database section found

  /** Loads all the data from Redis RDB file, and stores the same in the value
    * store. Returns the error as a string if there was any. If the file does
    * not exist, no processing happens with no error reported.
    * @param filePath
    *   The path to the database dump file
    */
  def loadFile(filePath: String): Option[String] =
    // TODO: Parse the ending section of the RDB file as well
    File
      .getStream(filePath)
      .flatMap(s =>
        println("Found a database dump. Using it to initialize the database...")
        val d = Decoder(s)

        readHeaderSection(d) match
          case Some(err) => Some(err) // Header section error
          case None =>
            readMetadata(d) match
              case Failure(ex) => Some(ex.toString) // Metadata reading error
              case Success(metadata) =>
                metadata.foreach((k, v) => println(s"[Metadata] $k -> $v"))
                readDatabaseSection(d) match
                  case Failure(ex) =>
                    Some(ex.toString) // Database section error
                  case Success(records) =>
                    records.foreach((k, v) => ServerState.addKey(k, v))
                    None // No errors, return None
      )
