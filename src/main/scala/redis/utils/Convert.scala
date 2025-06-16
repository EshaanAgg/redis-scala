package redis.utils

import java.nio.ByteBuffer
import java.nio.ByteOrder


object Convert:
    def getLENumber(buf: Array[Byte]): Long = 
        buf.length match
            case 1 => buf(0).toLong
            case 2 => ((buf(0) & 0xFF) << 8) | (buf(1) & 0xFF)
            case _ =>
                ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong
    
    def getBENumber(buf: Array[Byte]): Long =
        buf.length match
            case 1 => buf(0).toLong
            case 2 => ((buf(1) & 0xFF) << 8) | (buf(0) & 0xFF)
            case _ =>
                ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN).getLong

        
