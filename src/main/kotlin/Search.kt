import BoyerMooreHorspoolRaita.processBytes
import BoyerMooreHorspoolRaita.searchBytes
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

data class Substring(val start: Int, val end: Int, val text: String)

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

fun startsWith(buf: MappedByteBuffer, start: Int, needle: String): Boolean {
  for (j in start until start + needle.length) {
    if (j >= buf.limit() || buf[j].toChar() != needle[j - start]) {
      return false
    }
  }
  return true
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

fun isXMLAttributeEquals(rootElement: String, chunk: String, key: String, value: String): Boolean {
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

fun getAttributeValue(buf: MappedByteBuffer, i: Int): Substring? {
  val tagStart = lastIndexOf(buf, i, "<")
  if (startsWith(buf, tagStart, "<gen:stringAttribute ")) { // TODO support other namespaces
    val tagEnd = indexOf(buf, i, ">")
    var nts = tagEnd
    var valueStart = -1
    while (valueStart < 0) {
      val nextTag = findNextTag(buf, nts)
      if (nextTag.text == "gen:value") {
        valueStart = nextTag.end
        break
      } else if (nextTag.text == "/gen:stringAttribute") {
        break
      }
      nts = nextTag.end
    }

    return if (valueStart >= 0) {
      val valueEnd = indexOf(buf, valueStart, "<")
      Substring(valueStart + 1, valueEnd, substring(buf, valueStart + 1, valueEnd))
    } else {
      null
    }
  }
  return null
}

fun findNextTag(buf: MappedByteBuffer, start: Int): Substring {
  val s = indexOf(buf, start, "<")
  val sb = StringBuilder()
  var e = s + 1
  while (e < buf.limit() && buf[e].toChar() != '>') {
    sb.append(buf[e].toChar())
    e++
  }
  return Substring(s, e, sb.toString())
}

fun findChunk(buf: MappedByteBuffer, first: Int, last: Int): IntRange {
  val startStr = "<core:cityObjectMember"
  val s = lastIndexOf(buf, first, startStr)

  val endStr = "/core:cityObjectMember>"
  val e = indexOf(buf, last, endStr) + endStr.length

  return s..e
}

fun extractChunk(buf: MappedByteBuffer, first: Int, last: Int): Substring {
  val r = findChunk(buf, first, last)
  return Substring(r.first, r.last, substring(buf, r.first, r.last))
}

// strategy find 'key' in text, check if it's an attribute, get attribute
// value, compare with 'value', finally get complete chunk if value matches
fun run(path: String, key: String, value: String): List<String> {
  return run(path, key) { it.contains(value) }
}

fun run(path: String, key: String, matches: (String) -> Boolean): List<String> {
  return run(path, listOf(key)) { _, value -> matches(value) }
}

fun run(path: String, keys: List<String>, matches: (key: String, value: String) -> Boolean): List<String> {
  if (keys.isEmpty()) {
    return emptyList()
  }

  val start = System.currentTimeMillis()

  val raf = RandomAccessFile(path, "r")
  val channel = raf.channel
  val size = channel.size().coerceAtMost(Int.MAX_VALUE.toLong() - 100) // TODO 2GB MAX!
  val buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)

  // process keys
  val patterns = keys.map { """"$it"""".toByteArray() }
  val processedPatterns = patterns.map { processBytes(it) }

  var checked = 0
  var valueNotFound = 0
  var chunksSearchedAgain = 0
  var i = searchBytes(buf, 0, buf.limit(), patterns[0], processedPatterns[0])
  val result = mutableListOf<String>()
  while (i >= 0) {
    checked++
    var nextSearchPos = i + patterns[0].size

    var allValuesMatch = true
    var chunkRange: IntRange? = null
    for ((j, key) in keys.withIndex()) {
      val p = if (chunkRange != null) {
        // We already found a chunk (from searching for the first key). Search
        // inside the chunk again and try to find the next key.
        chunksSearchedAgain++
        searchBytes(buf, chunkRange.first, chunkRange.last, patterns[j], processedPatterns[j])
      } else {
        i
      }
      if (p == -1) {
        allValuesMatch = false
        break
      }

      val actualValue = getAttributeValue(buf, p)
      if (actualValue == null) {
        valueNotFound++
        break
      }

      if (matches(key, actualValue.text)) {
        chunkRange = findChunk(buf, actualValue.start, actualValue.end)
      } else {
        allValuesMatch = false
        break
      }
    }

    if (chunkRange != null) {
      if (allValuesMatch) {
        val chunk = substring(buf, chunkRange.first, chunkRange.last)
        result.add(chunk)
      }
      nextSearchPos = chunkRange.last
    }

    i = searchBytes(buf, nextSearchPos, buf.limit(), patterns[0], processedPatterns[0])
  }

  println(path)
  println("File size: $size")
  println("Keys: $keys")
  println("Time taken: ${System.currentTimeMillis() - start}")
  println("Values checked: $checked")
  println("Matches: ${result.size}")
  println("Misses: ${checked - result.size}")
  println("Times a chunk was searched again for another key-value pair: $chunksSearchedAgain")
  println("Places where 'key' was not a valid string attribute: $valueNotFound")

  return result
}

// strategy: find 'value' in text, get chunk around it, parse chunk with XML
// parser, finally compare attribute value
fun runParseXML(path: String, key: String?, value: String): List<String> {
  val start = System.currentTimeMillis()

  val raf = RandomAccessFile(path, "r")
  val channel = raf.channel
  val size = channel.size().coerceAtMost(Int.MAX_VALUE.toLong() - 100) // TODO 2GB MAX!
  val buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)

  val rootElement = readRootElement(buf)

  var checked = 0
  val pattern = value.toByteArray()
  val processedPattern = processBytes(pattern)
  var i = searchBytes(buf, 0, buf.limit(), pattern, processedPattern)
  val result = mutableListOf<String>()
  while (i >= 0) {
    checked++
    var nextSearchPos = i + pattern.size

    val chunk = extractChunk(buf, i, i)
    if (key == null || isXMLAttributeEquals(rootElement, chunk.text, key, value)) {
      result.add(chunk.text)
      nextSearchPos = chunk.end
    }

    i = searchBytes(buf, nextSearchPos, buf.limit(), pattern, processedPattern)
  }

  println(path)
  println("File size: $size")
  println("Key: $key")
  println("Value: $value")
  println("Time taken: ${System.currentTimeMillis() - start}")
  // println("Chunk bounds: $l")
  println("Chunks checked: $checked")
  println("Matches: ${result.size}")
  println("Misses: ${checked - result.size}")

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
        val minValue = ctx.queryParams().get("minValue")
        val maxValue = ctx.queryParams().get("maxValue")
        val singleFile = ctx.queryParams().get("singleFile").toBoolean()
        val response = ctx.response()
        if (key == null && value == null) {
          response.setStatusCode(400).end("Either [value], [key + value] or [key + minValue + maxValue] must be given")
        } else if ((minValue != null && maxValue == null) || (minValue == null && maxValue != null)) {
          response.setStatusCode(400).end("minValue and maxValue must both be given")
        } else if ((minValue != null || maxValue != null) && key == null) {
          response.setStatusCode(400).end("minValue and maxValue can only be used in combination with key")
        } else if (minValue == null && maxValue == null && value == null) {
          response.setStatusCode(400).end("Missing value")
        } else {
          val nMinValue = minValue?.toDouble()
          val nMaxValue = maxValue?.toDouble()

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
                val result = if (key == null) {
                  runParseXML(f, null, value)
                } else if (nMinValue != null && nMaxValue != null) {
                  run(f, key) {
                    it.toDoubleOrNull()?.let { actualValue ->
                      actualValue in nMinValue..nMaxValue
                    } ?: false
                  }
                } else {
                  run(f, key, value)
                }
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

// TODO caveat: only supports plain ASCII encoding, proper UTF-8 decoding might actually affect performance
// TODO caveat: if file is not well formatted we might search until the end (e.g. when looking for the end of a tag or chunk)
suspend fun main() {
  val vertx = Vertx.vertx()
  // vertx.deployVerticle(SearchVerticle()).await()

  val path = "/Users/mkraemer/code/ogc3dc/DA_WISE_GML_enhanced/DA12_3D_Buildings_Merged.gml"
  // should yield 1 chunk
  // runParseXML(path, "ownername", "Empire State Building")
  run(path, "ownername", "Empire State Building")

  // should yield 1179 chunks
  // runParseXML(path, "zipcode", "10019")
  run(path, "zipcode", "10019")

  run(path, "zipcode") { v ->
    v.toIntOrNull()?.let { zipcode ->
      zipcode in 10018..10020
    } ?: false
  }

  run(path, "numfloors") { v ->
    v.toDoubleOrNull()?.let { floors ->
      floors in 1.0..3.0
    } ?: false
  }

  // takes a lot longer because the attribute name is very short
  run(path, "cd", "104")

  run(path, "latitude") { v ->
    v.toDoubleOrNull()?.let { latitude ->
      latitude in 40.768548..40.768549
    } ?: false
  }

  run(path, "latitude") { v ->
    v.toDoubleOrNull()?.let { latitude ->
      latitude in 40.767..40.769
    } ?: false
  }

  run(path, listOf("latitude", "longitude", "address")) { k, v ->
    if (k == "address") {
      v == "1 Columbus Circle"
    } else {
      v.toDoubleOrNull()?.let { parsedValue ->
        if (k == "latitude") {
          parsedValue in 40.7684..40.7686
        } else {
          parsedValue in -73.984..-73.982
        }
      } ?: false
    }
  }

  // numeric attributes:
  // zipcode (integer), e.g. 10019
  // numfloors (integer, but encoded as float, e.g. 6.0000000)
  // latitude, 40.7685485
  // longitude, -73.9864944
}
