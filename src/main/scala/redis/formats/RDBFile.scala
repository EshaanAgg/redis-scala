package redis.formats

import redis.utils.File

import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object RDBFile:
  // Define methods on decoder to read datatypes specific to RDB file
  extension (d: Decoder)
    private def readInt: Try[Int] =
      d.readByte.flatMap(b =>
        val identifierBits = (b & 0b1100_0000) >> 6
        val otherBits = (b & 0b1100_0000)
        identifierBits match
          case 0 => Success(otherBits)
          case 1 => d.readByte.map(h => h << 6 | otherBits)
          case 2 =>
            d.readNBytes(4)
              .map(buf =>
                ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN).getInt
              )
          case 3 =>
            throw Exception(
              s"Unsupported starting byte for reading an integer: $b"
            )
      )

    private def readString: Try[String] =
      readInt.flatMap(len => d.readNBytes(len).map(_.mkString))

    private def expectByte(b: Int): Unit =
      val rb = d.readByte.get
      if rb != b then throw new Exception(s"Expected byte '$b', got '$rb'")

  private def readHeaderSection(d: Decoder): Option[String] =
    val supportedHeader = "REDIS0011"
    d.readNBytes(supportedHeader.length) match
      case Success(byteArr) =>
        if byteArr.sameElements(supportedHeader.getBytes)
        then None
        else
          Some(
            s"Required the header section to be '$supportedHeader', but received '${byteArr.mkString("Array(", ", ", ")")}'"
          )
      case Failure(ex) => Some(ex.toString)

  def readMetadata(d: Decoder): Try[Vector[(String, String)]] =
    def loop(acc: Vector[(String, String)]): Try[Vector[(String, String)]] =
      d.peekByte match
        case Some(0xfe) => Success(acc) // End of metadata
        case Some(_) =>
          val kvTry = for {
            key <- d.readString
            value <- d.readString
          } yield (key -> value)
          kvTry.flatMap(kv => loop(acc :+ kv))
        case None =>
          Failure(
            new RuntimeException(
              "Unexpected end of stream while reading metadata"
            )
          )

    d.expectByte(0xfa)
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

        val headerResult = readHeaderSection(d)
        if headerResult.isDefined
        then headerResult
        else
          readMetadata(d).foreach(arr => println(s"Metadata: ${arr.mkString}"))
          None
      )
