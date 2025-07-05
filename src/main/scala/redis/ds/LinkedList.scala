package redis.ds

import scala.annotation.tailrec

case class Node[T](
  value: T,
  var prev: Option[Node[T]] = None,
  var next: Option[Node[T]] = None
):
  def isHead: Boolean = prev.isEmpty

  def isTail: Boolean = next.isEmpty

  def setNext(nextNode: Node[T]): Unit =
    next = Some(nextNode)
    nextNode.prev = Some(this)

  def setPrev(prevNode: Node[T]): Unit =
    prev = Some(prevNode)
    prevNode.next = Some(this)

  override def toString: String = s"Node($value)"

case class LinkedList[T](
  var head: Option[Node[T]] = None,
  var tail: Option[Node[T]] = None,
  var length: Int = 0
):
  def isEmpty: Boolean = length == 0
  def nonEmpty: Boolean = !isEmpty

  def popHead: T =
    assert(!isEmpty, "popHead called on empty list")
    val h = head.get
    if h.next.isDefined then h.next.get.prev = None
    head = h.next
    length -= 1
    h.value

  def popTail: T =
    assert(!isEmpty, "popTail called on empty list")

    val t = tail.get
    if t.prev.isDefined then t.prev.get.next = None
    tail = t.prev
    length -= 1
    t.value

  /** Adds a new node with the given value to the end of the linked list. The
    * complexity of this operation is O(1).
    *
    * @param value
    *   the value to be added
    * @return
    *   the newly created node
    */
  def addTail(value: T): Node[T] =
    val newNode = Node(value)
    if isEmpty then
      head = Some(newNode)
      tail = Some(newNode)
    else
      assert(tail.isDefined, "Tail should be defined when list is not empty")
      tail.get.setNext(newNode)
      tail = Some(newNode)
    length += 1
    newNode

  /** Adds a new node with the given value to the beginning of the linked list.
    * The complexity of this operation is O(1).
    * @param value
    *   the value to be added
    * @return
    *   the newly created node
    */
  def addHead(value: T): Node[T] =
    val newNode = Node(value)
    if isEmpty then
      head = Some(newNode)
      tail = Some(newNode)
    else
      assert(head.isDefined, "Head should be defined when list is not empty")
      head.get.setPrev(newNode)
      head = Some(newNode)
    length += 1
    newNode

  /** Returns the appropriate idx in the range [0, length - 1] for the given idx
    * in range -inf to inf. Implements support for -ve indexes to reference
    * nodes from the end, and clipping for out of bounds indexes.
    *
    * @param idx
    *   The index to normalize
    * @return
    *   The normalized index
    */
  def getNormalizedIdx(idx: Int): Int =
    if idx < -length then 0
    else if idx < 0 then length + idx
    else if idx < length then idx
    else length - 1

  /** Traverses 'i' number of steps forward from the current 'cur' node.
    * @param i
    *   Number of steps to go forward.
    * @param cur
    *   The node to traverse from. Defaults to the head to the list.
    * @return
    */
  @tailrec
  private def traverseHead(i: Int, cur: Node[T] = head.get): Node[T] =
    if i == 0 then cur else traverseHead(i - 1, cur.next.get)

  /** Traverses 'i' number of steps backward from the current 'cur' node.
    *
    * @param i
    *   Number of steps to go back.
    * @param cur
    *   The node to traverse from. Defaults to the tail to the list.
    * @return
    */
  @tailrec
  private def traverseTail(i: Int, cur: Node[T] = tail.get): Node[T] =
    if i == 0 then cur else traverseTail(i - 1, cur.prev.get)

  /** Returns the element at the index idx in the current list. It is required
    * that the index is valid, that is between 0 and length - 1. Should not be
    * called on an empty list.
    *
    * The list is traversed from the nearer end for faster implementation.
    * @param idx
    *   The index to get
    * @return
    *   The fetched node
    */
  def apply(idx: Int): Node[T] =
    assert(
      0 <= idx && idx < length,
      s"Invalid idx = $idx accessed for linked list with length $length"
    )
    if idx <= (length - 1) / 2
    then traverseHead(idx)
    else traverseTail(length - idx - 1)

  /** Returns all the nodes between 'a' and 'b' in the list. The user must
    * ensure that both the nodes exist in the list and that node a <= node b in
    * the list.
    * @param a
    *   The starting node
    * @param b
    *   The ending node
    * @param acc
    *   The accumulated list till now
    * @return
    */
  @tailrec
  final def apply(
    a: Node[T],
    b: Node[T],
    acc: List[Node[T]] = Nil
  ): List[Node[T]] =
    if (a == b) then a +: acc
    else apply(a, b.prev.get, b +: acc)
