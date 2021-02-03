import com.fasterxml.aalto.stax.InputFactoryImpl
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerOptions
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import org.apache.commons.io.input.SequenceReader
import org.codehaus.staxmate.SMInputFactory
import java.io.RandomAccessFile
import java.io.StringReader
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

val XML_FACTORY = SMInputFactory(InputFactoryImpl())

// see https://en.wikipedia.org/wiki/Boyer%E2%80%93Moore_string-search_algorithm
fun indexOfBoyerMoore(haystack: MappedByteBuffer, needle: String): List<Int> {
  if (needle.isEmpty()) {
    return listOf(0)
  }

  val charTable = makeCharTable(needle)
  val offsetTable = makeOffsetTable(needle)

  val result = mutableListOf<Int>()
  var i = needle.length - 1
  var j: Int
  while (i < haystack.limit()) {
    j = needle.length - 1
    while (needle[j] == haystack[i].toChar()) {
      if (j == 0) {
        result.add(i)
        break
      }
      --i
      --j
    }
    i += offsetTable[needle.length - 1 - j].coerceAtLeast(charTable[haystack[i].toInt()])
  }

  return result
}

private fun makeCharTable(needle: String): IntArray {
  val alphabetSize = Character.MAX_VALUE.toInt() + 1 // 65536
  val table = IntArray(alphabetSize)

  for (i in table.indices) {
    table[i] = needle.length
  }

  for (i in 0 until needle.length - 2) {
    table[needle[i].toInt()] = needle.length - 1 - i
  }

  return table
}

private fun makeOffsetTable(needle: String): IntArray {
  val table = IntArray(needle.length)
  var lastPrefixPosition = needle.length

  for (i in needle.length downTo 1) {
    if (isPrefix(needle, i)) {
      lastPrefixPosition = i
    }
    table[needle.length - i] = lastPrefixPosition - i + needle.length
  }

  for (i in 0 until needle.length - 1) {
    val slen = suffixLength(needle, i)
    table[slen] = needle.length - 1 - i + slen
  }

  return table
}

private fun isPrefix(needle: String, p: Int): Boolean {
  var i = p
  var j = 0
  while (i < needle.length) {
    if (needle[i] != needle[j]) {
      return false
    }
    ++i
    ++j
  }
  return true
}

private fun suffixLength(needle: String, p: Int): Int {
  var len = 0
  var i = p
  var j = needle.length - 1
  while (i >= 0 && needle[i] == needle[j]) {
    len += 1
    --i
    --j
  }
  return len
}

fun indexOf(haystack: MappedByteBuffer, offset: Int, needle: String): Int {
  for (i in offset until haystack.limit() - needle.length) {
    for (j in needle.indices) {
      val c = haystack[i + j].toChar()
      if (c != needle[j]) {
        break
      }
      if (j == needle.length - 1) {
        return i
      }
    }
  }
  return -1
}

fun lastIndexOf(haystack: MappedByteBuffer, offset: Int, needle: String): Int {
  for (i in offset downTo 0) {
    for (j in needle.indices) {
      val c = haystack[i + j].toChar()
      if (c != needle[j]) {
        break
      }
      if (j == needle.length - 1) {
        return i
      }
    }
  }
  return -1
}

fun substring(buf: MappedByteBuffer, start: Int, end: Int): String {
  val tmp = ByteArray(end - start)
  for (j in start until end) {
    tmp[j - start] = buf[j]
  }
  return String(tmp)
}

fun readRootElement(buf: MappedByteBuffer): String {
  var start = -1
  for (i in 0 until buf.limit() - 1) {
    if (start == -1) {
      if (buf[i].toChar() == '<' && buf[i + 1].toChar() != '?') {
        start = i
      }
    } else {
      if (buf[i].toChar() == '>') {
        return substring(buf, start, i + 1)
      }
    }
  }

  return ""
}

fun isAttributeEquals(rootElement: String, chunk: String, key: String, value: String): Boolean {
  val rootElementReader = StringReader(rootElement)
  val endElementReader = StringReader("</core:CityModel>")
  val chunkReader = StringReader(chunk)
  val rootCursor = XML_FACTORY.rootElementCursor(SequenceReader(rootElementReader, chunkReader, endElementReader))
  rootCursor.advance()

  val rootChildCursor = rootCursor.childElementCursor()
  while (rootChildCursor.next != null) {
    if (rootChildCursor.localName == "cityObjectMember") {
      val buildingCursor = rootChildCursor.childElementCursor()
      while (buildingCursor.next != null) {
        if (buildingCursor.localName == "Building") {
          val attributeCursor = buildingCursor.childElementCursor()
          while (attributeCursor.next != null) {
            if (attributeCursor.localName == "stringAttribute") {
              if (attributeCursor.getAttrValue("name") == key) {
                val valueCursor = attributeCursor.childElementCursor()
                while (valueCursor.next != null) {
                  if (valueCursor.localName == "value") {
                    if (valueCursor.elemStringValue.contains(value)) {
                      return true
                    }
                  }
                }
                // we found the attribute but the value did not match
                return false
              }
            }
          }
        }
      }
    }
  }

  return false
}

fun run(path: String, key: String?, value: String): List<String> {
  val start = System.currentTimeMillis()

//  val i = File(path).inputStream()
//  i.use {
//    val buf = ByteArray(65536)
//    var r: Int
//    do {
//      r = i.read(buf)
//      for (n in 0 until r) {
//        val c = buf[n]
//        ++count
//      }
//    } while (r >= 0)
//  }

  val raf = RandomAccessFile(path, "r")
  val channel = raf.channel
  val size = channel.size().coerceAtMost(Int.MAX_VALUE.toLong() - 100) // TODO 2GB MAX!
  val buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)

  val rootElement = readRootElement(buf)

  val l = indexOfBoyerMoore(buf, value)
  val result = mutableListOf<String>()
  for (i in l) {
    val startStr = "<core:cityObjectMember"
    val s = lastIndexOf(buf, i, startStr)

    val endStr = "/core:cityObjectMember>"
    val e = indexOf(buf, i, endStr) + endStr.length

    val chunk = substring(buf, s, e)
    if (key == null || isAttributeEquals(rootElement, chunk, key, value)) {
      result.add(chunk)
    }
  }

  println(path)
  println("File size: $size")
  println("Key: $key")
  println("Value: $value")
  println("Time taken: ${System.currentTimeMillis() - start}")
  println("Chunk bounds: $l")
  println("Chunks checked: ${l.size}")
  println("Matches: ${result.size}")
  // println(e)

  return result
}

class SearchVerticle : CoroutineVerticle() {
  override suspend fun start() {
    val options = HttpServerOptions()
        .setCompressionSupported(true)
    val server = vertx.createHttpServer(options)
    val router = Router.router(vertx)

    router.route("/").handler { ctx ->
      launch {
        val key: String? = ctx.queryParams().get("key")
        val value = ctx.queryParams().get("value")
        val singleFile = ctx.queryParams().get("singleFile").toBoolean()
        val response = ctx.response()
        if (value == null) {
          response.setStatusCode(400).end()
        } else {
          val path = "/Users/mkraemer/code/ogc3dc/DA_WISE_GML_enhanced/"
          val fs = vertx.fileSystem()
          var files = fs.readDir(path).await()

          if (singleFile) {
            files = files.filter { it.endsWith("DA12_3D_Buildings_Merged.gml") }
          }

          response.isChunked = true

          val ds = files.map { f ->
            async {
              vertx.executeBlocking<Unit>({ p ->
                val result = run(f, key, value)
                for (r in result) {
                  response.write(r)
                }
               p.complete()
              }, false).await()
            }
          }
          ds.awaitAll()

          response.end()
        }
      }
    }

    val host = "localhost"
    val port = 6000
    server.requestHandler(router).listen(port, host).await()

    println("HTTP endpoint deployed to http://$host:$port")
  }
}

suspend fun main() {
  val vertx = Vertx.vertx()
  // vertx.deployVerticle(SearchVerticle())

  val path = "/Users/mkraemer/code/ogc3dc/DA_WISE_GML_enhanced/DA12_3D_Buildings_Merged.gml"
  run(path, "ownername", "Empire State Building")
}
