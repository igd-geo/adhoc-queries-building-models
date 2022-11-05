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

    private var indexArray: Array<Pair<Long, T>?> = arrayOf(initKey to initElement)

    // Number of elements in this index. We always have an init element.
    var size = 1

    // Flag to indicate if the index has been modified after loading.
    var indexChanged = false

    @Throws(IOException::class)
    private fun writeObject(objectOutputStream: ObjectOutputStream) {
        objectOutputStream.writeLong(indexedBoundary)
        objectOutputStream.writeInt(size)
        var currentNode: Node<T>? = first
        while (currentNode != null) {
            objectOutputStream.writeLong(currentNode.key)
            objectOutputStream.writeObject(currentNode.element)
            currentNode = currentNode.next
        }
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    private fun readObject(objectInputStream: ObjectInputStream) {
        val start = System.currentTimeMillis()
        indexedBoundary = objectInputStream.readLong()
        size = objectInputStream.readInt()
        indexArray = arrayOfNulls(size)
        var lastNode: Node<T>? = null
        for (i in 0 until size) {
            val key = objectInputStream.readLong()
            val element = objectInputStream.readObject() as T
            indexArray[i] = key to element
            val node = Node(key, element, null)
            if (first == null) first = node
            lastNode?.next = node
            lastNode = node
        }
        val readDuration = System.currentTimeMillis() - start
        log("INDEX: Deserialized $size entries for the index in $readDuration ms")
    }

    fun add(key: Long, element: T) {
        indexChanged = true
        size++
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

    private fun binarySearch(key: Long): Int {
        var low = 0
        var high = indexArray.size - 1

        while (low <= high) {
            val middle = (low + high) / 2
            val middleValue = indexArray[middle]!!.first
            if (middleValue < key) {
                low = middle + 1
            } else if (middleValue > key) {
                high = middle - 1
            } else {
                return middle // key found
            }
        }
        return -(low + 1) // key not found
    }

    fun sublist(minKey: Long, maxKey: Long, filterFunction: (T) -> Boolean): List<T> {
        if (indexChanged) throw RuntimeException("Index was modified")

        val startIndex = binarySearch(minKey).let {
            if (it < 0) -1 * it - 1
            else it
        }
        val endIndex = binarySearch(maxKey).let {
            if (it < 0) -1 * it - 2
            else it
        }

        if (startIndex > endIndex) return emptyList()

        val result = LinkedList<T>()
        for (i in startIndex..endIndex) {
            if (filterFunction(indexArray[i]!!.second)) result.add(indexArray[i]!!.second)
        }
        return result

        // Search using the list representation of the index.
        // This code is a little bit slower than the array based implementation above.
//        var currentElement = first
//        while (currentElement.key < minKey) {
//            if (currentElement.next == null) return LinkedList<T>()
//            currentElement = currentElement.next!!
//        }
//
//        val result = LinkedList<T>()
//        while (currentElement.key <= maxKey) {
//            if (filterFunction(currentElement.element)) {
//                result.add(currentElement.element)
//            }
//            if (currentElement.next == null) return result
//            currentElement = currentElement.next!!
//        }
//        return result
    }
}
