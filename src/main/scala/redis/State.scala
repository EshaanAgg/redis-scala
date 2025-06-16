package redis

import redis.RESP2.DataType

import java.time.Instant
import scala.collection.concurrent.TrieMap

case class StoreVal(data: DataType, exp: Option[Instant])

object ServerState:
  private val store: TrieMap[String, StoreVal] = new TrieMap()

  def addKey(k: String, v: StoreVal): TrieMap[String, StoreVal] =
    store += k -> v

  def get(k: String): Option[StoreVal] =
    store.get(k).flatMap(v =>
      if v.exp.isDefined && v.exp.get.isBefore(Instant.now()) then
        store -= k
        None
      else Some(v)
    )