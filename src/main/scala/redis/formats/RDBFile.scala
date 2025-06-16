package redis.formats

import redis.utils.File

import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object RDBFile:
  private type Metadata = (String, String)
  private val MetadateSectionByte: Byte = 0xfa.toByte

  // Define methods on decoder to read datatypes specific to RDB file
  extension (d: Decoder)
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
              .map(
                ByteBuffer.wrap(_).order(ByteOrder.BIG_ENDIAN).getInt
              )
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
            .map(buf =>
              // Match on the length to determine how to convert to Int
              buf.length match
                case 1 => buf(0).toString
                case 2 => (buf(0) << 8) + (buf(1)).toString
                case 4 =>
                  ByteBuffer
                    .wrap(buf)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt
                    .toString
                case _ =>
                  throw new DecoderException(
                    s"Unexpected byte length: ${buf.length}"
                  )
            )
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

  /** Loads all the data from Redis RDB file, and stores the same in the value
    * store. Returns the error as a string if there was any. If the file does
    * not exist, no processing happens with no error reported.
    * @param filePath
    *   The path to the database dump file
    */
  def loadFile(filePath: String): Option[String] =
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
                None
      )
