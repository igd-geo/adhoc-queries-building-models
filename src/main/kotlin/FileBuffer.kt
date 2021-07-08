import java.io.Closeable
import java.io.RandomAccessFile

class FileBuffer(path: String, private val chunkSize: Int = 1024 * 1024) : Closeable {
  private val file: RandomAccessFile = RandomAccessFile(path, "r")
  private val buffer = ByteArray(chunkSize)
  private var pos = 0L
  private var read = 0

  private fun align(i: Long): Long = i - (i % chunkSize)

  val size by lazy { file.length() }

  operator fun get(i: Long): Byte {
    if (i < pos || i >= pos + read) {
      pos = align(i)
      file.seek(pos)
      read = file.read(buffer)
    }
    return buffer[(i - pos).toInt()]
  }

  override fun close() {
    file.close()
  }
}
