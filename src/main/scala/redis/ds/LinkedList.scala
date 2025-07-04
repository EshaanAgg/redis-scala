package redis.ds

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
