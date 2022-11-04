import java.io.*
import java.util.*

class Index<T: PrimitiveSerializable>(initKey: Long, initElement: T) {
    companion object {
        fun<T: PrimitiveSerializable> readIndex(file: File, factory: (DataInputStream) -> T ): Index<T> {
            val fileInputStream = FileInputStream(file)
            val dataInputStream = DataInputStream(fileInputStream)

            val indexBoundary = dataInputStream.readLong()
            val indexSize = dataInputStream.readInt()

            val firstKey = dataInputStream.readLong()
            val firstElement = factory(dataInputStream)
            val index = Index(firstKey, firstElement)
            index.indexedBoundary = indexBoundary

            var currentNode = index.first
            for (i in 1 until indexSize) {
                val key = dataInputStream.readLong()
                val element = factory(dataInputStream)
                val nextNode = Node(key, element, null)
                currentNode.next = nextNode
                currentNode = nextNode

            }

            dataInputStream.close()
            log("Deserialized $indexSize entries for the index")
            return index
        }
    }

    private class Node<T: PrimitiveSerializable>(val key: Long, val element: T, var next: Node<T>?)

    // Byte position in buf until where the index exists. (exclusive)
    var indexedBoundary = 0L

    // The first entry (lowest morton code)
    private var first = Node(initKey, initElement, null)

    /**
     * Writes this index to the passed file.
     */
    fun writeIndex(file: File) {
        var nextElementToBeAddedToIndexFile: Node<T>? = first
        val fileAlreadyExisting = file.exists()
        val randomAccessFile = RandomAccessFile(file, "rw")
        if (fileAlreadyExisting) {
            // We already have an old version of the index stored. Only append the new index entries.
            randomAccessFile.seek(0)
            val oldIndexBoundary = randomAccessFile.readLong()
            val oldIndexSize = randomAccessFile.readInt()

            val currentSize = size()
            if (currentSize < oldIndexSize || indexedBoundary < oldIndexBoundary) {
                throw RuntimeException("Stored index is larger than current one. $oldIndexSize vs $currentSize and $oldIndexBoundary vs $indexedBoundary")
            }

            if (oldIndexBoundary != indexedBoundary && oldIndexSize == currentSize ||
                oldIndexBoundary == indexedBoundary && oldIndexSize != currentSize) {
                throw RuntimeException("Unexpected index dimensions. $oldIndexSize vs $currentSize and $oldIndexBoundary vs $indexedBoundary")
            }

            // Nothing to do if the index has not changed.
            if (oldIndexSize == currentSize) return

            // Override the index dimensions in the file.
            randomAccessFile.seek(0)
            randomAccessFile.writeLong(indexedBoundary)
            randomAccessFile.writeInt(size())

            // Append the new index entries.
            randomAccessFile.seek(randomAccessFile.length())
            for (i in 0 until oldIndexSize) {
                nextElementToBeAddedToIndexFile = nextElementToBeAddedToIndexFile!!.next
            }
        } else {
            // This is a new file
            randomAccessFile.seek(0)
            randomAccessFile.writeLong(indexedBoundary)
            randomAccessFile.writeInt(size())
        }

        // Write remaining index entries
        val fileOutputStream = FileOutputStream(randomAccessFile.fd)
        val dataOutputStream = DataOutputStream(fileOutputStream)
        while (nextElementToBeAddedToIndexFile != null) {
            dataOutputStream.writeLong(nextElementToBeAddedToIndexFile.key)
            nextElementToBeAddedToIndexFile.element.write(dataOutputStream)
            nextElementToBeAddedToIndexFile = nextElementToBeAddedToIndexFile.next
        }
        dataOutputStream.flush()
        dataOutputStream.close()
    }

    /**
     * Adds a new entry to this index.
     */
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

    /**
     * Get all entries of the index between minKey (inclusive) and maxKey (inclusive)
     */
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

    /**
     * Number of entries in this index
     */
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