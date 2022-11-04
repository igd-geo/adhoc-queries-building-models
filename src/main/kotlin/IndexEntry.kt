import java.io.DataInputStream
import java.io.DataOutputStream

class IndexEntry(val boundingBox: BoundingBox,
                 val chunkStart: Long,
                 val chunkEnd: Long): PrimitiveSerializable {
    companion object {
        fun read(dataInputStream: DataInputStream): IndexEntry {
            val minX = dataInputStream.readDouble()
            val minY = dataInputStream.readDouble()
            val maxX = dataInputStream.readDouble()
            val maxY = dataInputStream.readDouble()
            val chunkStart = dataInputStream.readLong()
            val chunkEnd = dataInputStream.readLong()
            return IndexEntry(BoundingBox(minX, minY, maxX, maxY), chunkStart, chunkEnd)
        }
    }

    override fun write(dataOutputStream: DataOutputStream) {
        dataOutputStream.writeDouble(boundingBox.minX)
        dataOutputStream.writeDouble(boundingBox.minY)
        dataOutputStream.writeDouble(boundingBox.maxX)
        dataOutputStream.writeDouble(boundingBox.maxY)
        dataOutputStream.writeLong(chunkStart)
        dataOutputStream.writeLong(chunkEnd)
    }
}
