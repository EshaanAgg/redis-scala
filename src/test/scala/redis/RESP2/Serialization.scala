package redis.RESP2

import java.io.ByteArrayInputStream
import scala.util.Success

import DataType._

class Serialization extends munit.FunSuite:

  val simpleTestCases: List[(DataType, String)] =
    List(
      SimpleString("PING") -> "+PING\r\n",
      SimpleString("pong") -> "+pong\r\n",
      BulkString("PING") -> "$4\r\nPING\r\n",
      BulkString("abcdef") -> "$6\r\nabcdef\r\n",
      BulkString("") -> "$0\r\n\r\n",
      BulkString(None) -> "$-1\r\n",
      Error("error") -> "-error\r\n",
      Error("with space") -> "-with space\r\n",
      Integer(0) -> ":0\r\n",
      Integer(1234) -> ":1234\r\n",
      Integer(-1234) -> ":-1234\r\n",
      Array(List()) -> "*0\r\n",
      Array(
        List(
          BulkString("hello"),
          BulkString("world")
        )
      ) -> "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
      Array(
        List(
          Integer(1),
          Integer(2),
          Integer(3)
        )
      ) -> "*3\r\n:1\r\n:2\r\n:3\r\n",
      Array(
        List(
          Integer(1),
          Integer(2),
          Integer(3),
          Integer(4),
          BulkString("hello")
        )
      ) -> "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n",
      Array(
        List(
          Array(
            List(
              Integer(1),
              Integer(2),
              Integer(3)
            )
          ),
          Array(
            List(
              SimpleString("Hello"),
              Error("World")
            )
          )
        )
      ) -> "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
      Boolean.True -> "#t\r\n",
      Boolean.False -> "#f\r\n"
    )

  val nestedCases: List[DataType] = List(
    Array(
      List(
        Array(
          List(
            Array(
              List(
                Integer(42),
                BulkString("deep")
              )
            )
          )
        ),
        BulkString("end")
      )
    ),
    Array(
      List(
        SimpleString("nested"),
        Array(
          List(
            Integer(1),
            Array(
              List(
                BulkString("deep"),
                Error("nope")
              )
            )
          )
        ),
        Boolean.True,
        Error("Err"),
        Array(None)
      )
    )
  )

  val allCases: Seq[DataType] = simpleTestCases.map(_._1) ::: nestedCases

  test("encoding") {
    simpleTestCases.foreach((cls, expectedOutput) =>
      assertEquals(cls.encode, expectedOutput, s"Failed encoding for: $cls")
    )
  }

  test("decoding") {
    simpleTestCases.foreach((cls, encodedBytes) => {
      val stream = new ByteArrayInputStream(encodedBytes.getBytes())
      assertEquals(
        DataType(stream),
        Success(cls),
        s"Failed decoding for $encodedBytes"
      )
    })
  }

  test("roundtrip (encode -> decode -> same object)") {
    allCases.foreach { original =>
      val encoded = original.encode
      val stream = new ByteArrayInputStream(encoded.getBytes())
      val roundTripped = DataType(stream)

      assertEquals(
        roundTripped,
        Success(original),
        s"Failed roundtrip for: $original"
      )
    }
  }

end Serialization
