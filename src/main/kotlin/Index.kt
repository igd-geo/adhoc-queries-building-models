import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.util.*

class Index<T>(initKey: Long, initElement: T): Serializable {
    companion object {
        const val serialVersionUID = 7829136421241571165L
    }

    class Node<T>(val key: Long, val element: T, var next: Node<T>?): Serializable

    // Byte position in buf until where the index exists. (exclusive)
    var indexedBoundary = 0L

    // The first entry (lowest morton code)
    private var first = Node(initKey, initElement, null)

    @Throws(IOException::class)
    private fun writeObject(objectOutputStream: ObjectOutputStream) {
        objectOutputStream.writeLong(indexedBoundary)
        objectOutputStream.writeInt(size())
        var currentNode: Node<T>? = first
        while (currentNode != null) {
            objectOutputStream.writeLong(currentNode.key)
            objectOutputStream.writeObject(currentNode.element)
            currentNode = currentNode.next
        }
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    private fun readObject(objectInputStream: ObjectInputStream) {
        indexedBoundary = objectInputStream.readLong()
        val size = objectInputStream.readInt()
        var lastNode: Node<T>? = null
        for (i in 0 until size) {
            val key = objectInputStream.readLong()
            val element = objectInputStream.readObject() as T
            val node = Node(key, element, null)
            if (first == null) first = node
            lastNode?.next = node
            lastNode = node
        }

        log("Deserialized $size entries for the index")
    }

    fun add(key: Long, element: T) {
        if (key < first.key) {
            first = Node(key, element, first)
            return
        }

        var currentElement = first
        // There is a next entry AND the next entry is still smaller than the new entry.
        while (currentElement.next != null && (currentElement.next?.key?.compareTo(key) ?: 0) < 0) {
            currentElement = currentElement.next!!
        }
        val newNode = Node(key, element, currentElement.next)
        currentElement.next = newNode
    }

    fun sublist(minKey: Long, maxKey: Long): LinkedList<T> {
        var currentElement = first
        while (currentElement.key < minKey) {
            if (currentElement.next == null) return LinkedList<T>()
            currentElement = currentElement.next!!
        }

        val result = LinkedList<T>()
        while (currentElement.key <= maxKey) {
            result.add(currentElement.element)
            if (currentElement.next == null) return result
            currentElement = currentElement.next!!
        }
        return result
    }

    fun size(): Int {
        var counter = 0
        var currentElement: Node<T>? = first
        while (currentElement != null) {
            counter++
            currentElement = currentElement.next
        }
        return counter
    }
}