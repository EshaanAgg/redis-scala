package redis.utils

import scala.util.Try

object Common:
  def sequenceTries[T](arr: Seq[Try[T]]): Try[List[T]] =
    arr.foldRight(Try(List.empty[T])) { (tryElem, acc) =>
      for
        x <- tryElem
        xs <- acc
      yield x :: xs
    }
