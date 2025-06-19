package redis.utils

import java.nio.ByteBuffer
import java.nio.ByteOrder

object Convert:
  def getLENumber(buf: Array[Byte]): Long =
    buf.length match
      case 1 => buf(0).toLong
      case 2 => ((buf(0) & 0xff) << 8) | (buf(1) & 0xff)
      case 4 => ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
      case 8 => ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported length for getLENumber: ${buf.length}"
        )

  def getBENumber(buf: Array[Byte]): Long =
    buf.length match
      case 1 => buf(0).toInt
      case 2 => ((buf(1) & 0xff) << 8) | (buf(0) & 0xff)
      case 4 => ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN).getInt
      case 8 => ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN).getLong
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported length for getBENumber: ${buf.length}"
        )
