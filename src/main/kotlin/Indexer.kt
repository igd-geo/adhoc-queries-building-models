import BoyerMooreHorspoolRaita.processBytes
import BoyerMooreHorspoolRaita.searchBytes
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.lang.Double.max
import java.lang.Double.min

class Indexer(path: String) {

    data class IndexEntry(val boundingBox: BoundingBox, val chunkStart: Long, val chunkEnd: Long): Serializable

    companion object {
        const val PRECISION = 2 // cm resolution
    }

    private val buf = FileBuffer(path)
    private val rootElement = readRootElement(buf)
    private val namespacePrefixes = extractNamespacePrefixes(rootElement)

    private val gmlNamespacePrefix = namespacePrefixes[Namespace.GML] ?: ""
    private val posListPattern = "<${gmlNamespacePrefix}posList".toByteArray()
    private val processedPosListPattern = processBytes(posListPattern)

    private val serializedIndex = File("$path.index")
    var index: Index<IndexEntry>? = null

    init {
        loadIndexIfExisting()
    }

    private fun loadIndexIfExisting() {
        if (!serializedIndex.exists()) {
            index = null
            return
        }
        val fileInputStream = FileInputStream(serializedIndex)
        val objectInputStream = ObjectInputStream(fileInputStream)
        index = objectInputStream.readObject() as Index<IndexEntry>
        objectInputStream.close()
    }

    fun saveIndex() {
        if (index?.indexChanged == false) return
        val fileOutputStream = FileOutputStream(serializedIndex)
        val objectOutputStream = ObjectOutputStream(fileOutputStream)
        objectOutputStream.writeObject(index)
        objectOutputStream.flush()
        objectOutputStream.close()
    }

    /**
     * Get the byte range of the next chunk after the passed startPos.
     */
    private fun getNextChunk(startPos: Long): LongRange? {
        val namespacePrefix = namespacePrefixes[Namespace.CITYGML] ?: ""
        val startStr = "<${namespacePrefix}cityObjectMember"
        val s = indexOf(buf, startPos, startStr)
        if (s < 0) return null

        val endStr = "/${namespacePrefix}cityObjectMember>"
        val e = indexOf(buf, startPos + startStr.length, endStr) + endStr.length
        if (e < 0) return null

        return s..e
    }

    /**
     * Get the bounding box around the posList starting at byte position i.
     */
    private fun getPosListBoundingBox(i: Long): Pair<Long, BoundingBox> {
        val posListStart = indexOf(buf, i, ">") + 1

        val sb = StringBuilder()
        var j = posListStart // Current read position
        var n = 0 // Which type is currently read (x or y coordinate)
        var minX = Double.MAX_VALUE
        var minY = Double.MAX_VALUE
        var maxX = -1 * Double.MAX_VALUE
        var maxY = -1 * Double.MAX_VALUE

        // skip spaces at the beginning
        while (j < buf.size && buf[j].toInt().toChar() == ' ') j++

        while (j < buf.size) {
            val c = buf[j].toInt().toChar()

            if (c == '<' && sb.isBlank()) {
                // we're at the end of the posList and did not find any more coordinates
                break
            }

            if (c == ' ' || c == '<') {
                when (n) {
                    0 -> {
                        val x = sb.toString().toDouble()
                        minX = min(minX, x)
                        maxX = max(maxX, x)
                        n++
                    }
                    1 -> {
                        val y = sb.toString().toDouble()
                        minY = min(minY, y)
                        maxY = max(maxY, y)
                        n++
                    }
                    2 -> {
                        // don't parse z
                        n = 0
                    }
                }

                if (c == '<') {
                    // end of pos list
                    break
                }

                sb.clear()
                // skip other spaces
                while (j < buf.size && buf[j].toInt().toChar() == ' ') j++
            } else {
                sb.append(c)
                ++j
            }
        }

        return j to BoundingBox(minX, minY, maxX, maxY)
    }

    /**
     * Get the bounding box around the chunk defined by the passed byte range.
     * This is a bounding box around all posLists in this chunk.
     */
    private fun getChunkBoundingBox(chunk: LongRange): BoundingBox {
        var minX = Double.MAX_VALUE
        var minY = Double.MAX_VALUE
        var maxX = -1 * Double.MAX_VALUE
        var maxY = -1 * Double.MAX_VALUE

        // Get the byte position of the first hit.
        var i = searchBytes(buf, chunk.first, chunk.last, posListPattern, processedPosListPattern)
        while (i >= 0) {
            var nextSearchPos = i + posListPattern.size

            val posListResult = getPosListBoundingBox(nextSearchPos)
            nextSearchPos = posListResult.first
            minX = min(minX, posListResult.second.minX)
            minY = min(minY, posListResult.second.minY)
            maxX = max(maxX, posListResult.second.maxX)
            maxY = max(maxY, posListResult.second.maxY)

            i = searchBytes(buf, nextSearchPos, chunk.last, posListPattern, processedPosListPattern)
        }

        return BoundingBox(minX, minY, maxX, maxY)
    }

    /**
     * Add the passed chunk with the passed bounding box to the index.
     */
    private fun addToIndex(chunk: LongRange, boundingBox: BoundingBox) {
        val mortonCode = MortonCode.encode(boundingBox.minX.toFloat(), boundingBox.minY.toFloat(), PRECISION)
        val indexEntry = IndexEntry(boundingBox, chunk.first, chunk.last)
        if (index == null) index = Index(mortonCode, indexEntry)
        else index!!.add(mortonCode, indexEntry)
    }

    /**
     * Add the next passed number of chunks to the index.
     */
    fun index(maxChunks: Int = Int.MAX_VALUE) {
        val start = System.currentTimeMillis()
        var indexedChunksCounter = 0
        var chunk = getNextChunk(index?.indexedBoundary ?: 0L)
        while (chunk != null && indexedChunksCounter < maxChunks) { // There are more chunks
            val boundingBox = getChunkBoundingBox(chunk)
            addToIndex(chunk, boundingBox)
            indexedChunksCounter++

            val nextBytePosition = chunk.last + 1
            index?.indexedBoundary = nextBytePosition
            chunk = getNextChunk(nextBytePosition)
        }

        val duration = System.currentTimeMillis() - start
        log("$indexedChunksCounter chunks indexed in $duration ms. Index size is now ${index?.size}")
    }

    fun find(boundingBox: BoundingBox): List<String> {
        val start = System.currentTimeMillis()
        val minKey = MortonCode.encode(boundingBox.minX.toFloat(), boundingBox.minY.toFloat(), PRECISION)
        val maxKey = MortonCode.encode(boundingBox.maxX.toFloat(), boundingBox.maxY.toFloat(), PRECISION)
        val indexHits = index?.sublist(minKey, maxKey) {
            // Use only the hits that are completely in the searched bounding box
            boundingBox.contains(it.boundingBox)
        } ?: return emptyList()

        // Get the corresponding chunk
        val results = indexHits.map {
                val chunkRange = it.chunkStart..it.chunkEnd
                val chunk = substring(buf, chunkRange.first, chunkRange.last)
                chunk
            }
        val duration = System.currentTimeMillis() - start
        log("INDEX: Found ${results.size} hits in $duration ms")
        return results
    }
}
