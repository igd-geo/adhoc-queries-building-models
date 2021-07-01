import BoyerMooreHorspoolRaita.processBytes
import BoyerMooreHorspoolRaita.searchBytes
import com.fasterxml.aalto.stax.InputFactoryImpl
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.codehaus.staxmate.SMInputFactory
import java.io.File
import java.io.StringReader
import kotlin.math.roundToInt

val XML_FACTORY = SMInputFactory(InputFactoryImpl())

const val DEFAULT_WARMUP_RUNS = 2
const val DEFAULT_BENCH_RUNS = 5

const val NAMESPACE_GML = "http://www.opengis.net/gml"
const val NAMESPACE_CITYGML_1_0 = "http://www.opengis.net/citygml/1.0"
const val NAMESPACE_CITYGML_2_0 = "http://www.opengis.net/citygml/2.0"
const val NAMESPACE_GENERICS_1_0 = "http://www.opengis.net/citygml/generics/1.0"
const val NAMESPACE_GENERICS_2_0 = "http://www.opengis.net/citygml/generics/2.0"

enum class Namespace {
  GML,
  CITYGML,
  GENERICS
}

var runType = "BENCH"
var run = 0
var maxRuns = 10
const val LOG_FILE = "out.log"

fun log(msg: String) {
  synchronized(LOG_FILE) {
    val line = "[$runType $run/$maxRuns] $msg\n"
    print(line)
    File(LOG_FILE).appendText(line)
  }
}

data class SearchResult(val matches: List<String>, val filesize: Long,
    val keys: List<String> = emptyList(), val values: List<String> = emptyList(),
    val bbox: BoundingBox? = null, val duration: Long,
    val chunksChecked: Int, val posListsChecked: Int = 0,
    val chunksSearchedAgain: Int = 0, val valueNotFound: Int = 0)

data class Substring(val start: Long, val end: Long, val text: String)
data class BoundingBox(val minX: Double, val minY: Double, val maxX: Double, val maxY: Double) {
  fun contains(other: BoundingBox): Boolean {
    val rx = minX..maxX
    val ry = minY..maxY
    return other.minX in rx && other.maxX in rx && other.minY in ry && other.maxY in ry
  }

  fun contains(x: Double, y: Double): Boolean {
    return x in minX..maxX && y in minY..maxY
  }
}

fun indexOf(haystack: FileBuffer, offset: Long, needle: String): Long {
  for (i in offset until haystack.size - needle.length) {
    for (j in needle.indices) {
      val c = haystack[i + j].toInt().toChar()
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

fun lastIndexOf(haystack: FileBuffer, offset: Long, needle: String): Long {
  for (i in offset downTo 0) {
    for (j in needle.indices) {
      val c = haystack[i + j].toInt().toChar()
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

fun substring(buf: FileBuffer, start: Long, end: Long): String {
  val tmp = ByteArray((end - start).toInt())
  for (j in start until end) {
    tmp[(j - start).toInt()] = buf[j]
  }
  return String(tmp)
}

fun startsWith(buf: FileBuffer, start: Long, needle: String): Boolean {
  for (j in start until start + needle.length) {
    if (j >= buf.size || buf[j].toInt().toChar() != needle[(j - start).toInt()]) {
      return false
    }
  }
  return true
}

fun readRootElement(buf: FileBuffer): String {
  var start = -1L
  for (i in 0 until buf.size - 1) {
    if (start == -1L) {
      if (buf[i].toInt().toChar() == '<' && buf[i + 1].toInt().toChar() != '?') {
        start = i
      }
    } else {
      if (buf[i].toInt().toChar() == '>') {
        return substring(buf, start, i + 1)
      }
    }
  }

  return ""
}

fun extractNamespacePrefixes(rootElement: String): Map<Namespace, String> {
  val rootCursor = XML_FACTORY.rootElementCursor(StringReader(rootElement))
  rootCursor.advance()
  val result = mutableMapOf<Namespace, String>()
  for (i in 0 until rootCursor.streamReader.namespaceCount) {
    val namespace = when (rootCursor.streamReader.getNamespaceURI(i)) {
      NAMESPACE_GML -> Namespace.GML
      NAMESPACE_CITYGML_1_0, NAMESPACE_CITYGML_2_0 -> Namespace.CITYGML
      NAMESPACE_GENERICS_1_0, NAMESPACE_GENERICS_2_0 -> Namespace.GENERICS
      else -> null
    }
    if (namespace != null) {
      result[namespace] = rootCursor.streamReader.getNamespacePrefix(i)?.let {
        if (it.isEmpty()) it else "$it:" } ?: ""
    }
  }
  return result
}

fun getAttributeKey(buf: FileBuffer, i: Long, genNamespacePrefix: String): Substring? {
  val tagStart = lastIndexOf(buf, i, "<")
  if (startsWith(buf, tagStart, "<${genNamespacePrefix}value")) {
    val stringAttributeStart = lastIndexOf(buf, tagStart, "<${genNamespacePrefix}stringAttribute")
    val nameStart = indexOf(buf, stringAttributeStart, "name=\"")
    val nameEnd = indexOf(buf, nameStart + 6, "\"")
    return Substring(nameStart + 6, nameEnd, substring(buf, nameStart + 6, nameEnd))
  }
  return null
}

fun getAttributeValue(buf: FileBuffer, i: Long, genNamespacePrefix: String): Substring? {
  val tagStart = lastIndexOf(buf, i, "<")
  if (startsWith(buf, tagStart, "<${genNamespacePrefix}stringAttribute ")) {
    val tagEnd = indexOf(buf, i, ">")
    var nts = tagEnd
    var valueStart = -1L
    while (valueStart < 0L) {
      val nextTag = findNextTag(buf, nts)
      if (nextTag.text == "${genNamespacePrefix}value") {
        valueStart = nextTag.end
        break
      } else if (nextTag.text == "/${genNamespacePrefix}stringAttribute") {
        break
      }
      nts = nextTag.end
    }

    return if (valueStart >= 0L) {
      val valueEnd = indexOf(buf, valueStart, "<")
      Substring(valueStart + 1, valueEnd, substring(buf, valueStart + 1, valueEnd))
    } else {
      null
    }
  }
  return null
}

fun intersectsPosList(buf: FileBuffer, i: Long, bbox: BoundingBox): Long {
  val posListStart = indexOf(buf, i, ">") + 1

  val sb = StringBuilder()
  var j = posListStart
  var n = 0
  var x = 0.0
  var y = 0.0
  while (j < buf.size) {
    // look for end of next number
    val c = buf[j].toInt().toChar()
    if (c == ' ' || c == '<') {
      when (n) {
        0 -> x = sb.toString().toDouble()
        1 -> y = sb.toString().toDouble()
        2 -> { /* skip z */ }
        3 -> {
          if (bbox.contains(x, y)) {
            // stop as soon as we find a match
            return j
          }
          n = 0
        }
      }
      ++n

      if (c == '<') {
        // end of pos list
        break
      }

      sb.clear()
      // skip other spaces
      while (j < buf.size && buf[j].toInt().toChar() == ' ') j++
    } else {
      sb.append(c)
    }
    ++j
  }

  return -1
}

fun findNextTag(buf: FileBuffer, start: Long): Substring {
  val s = indexOf(buf, start, "<")
  val sb = StringBuilder()
  var e = s + 1
  while (e < buf.size && buf[e].toInt().toChar() != '>') {
    sb.append(buf[e].toInt().toChar())
    e++
  }
  return Substring(s, e, sb.toString())
}

fun findChunk(buf: FileBuffer, first: Long, last: Long,
    namespacePrefixes: Map<Namespace, String>): LongRange {
  val namespacePrefix = namespacePrefixes[Namespace.CITYGML] ?: ""

  val startStr = "<${namespacePrefix}cityObjectMember"
  val s = lastIndexOf(buf, first, startStr)

  val endStr = "/${namespacePrefix}cityObjectMember>"
  val e = indexOf(buf, last, endStr) + endStr.length

  return s..e
}

fun extractChunk(buf: FileBuffer, first: Long, last: Long,
    namespacePrefixes: Map<Namespace, String>): Substring {
  val r = findChunk(buf, first, last, namespacePrefixes)
  return Substring(r.first, r.last, substring(buf, r.first, r.last))
}

// strategy find 'key' in text, check if it's an attribute, get attribute
// value, compare with 'value', finally get complete chunk if value matches
fun run(path: String, key: String, value: String): SearchResult {
  return run(path, key) { it.contains(value) }
}

fun run(path: String, key: String, matches: (String) -> Boolean): SearchResult {
  return run(path, listOf(key)) { _, value -> matches(value) }
}

fun run(path: String, keys: List<String>, bbox: BoundingBox? = null,
    matcher: (key: String, value: String) -> Boolean): SearchResult {
  log(path)

  val start = System.currentTimeMillis()

  val buf = FileBuffer(path)

  val rootElement = readRootElement(buf)
  val namespacePrefixes = extractNamespacePrefixes(rootElement)

  // process keys
  val patterns = keys.map { """"$it"""".toByteArray() }
  val processedPatterns = patterns.map { processBytes(it) }

  var checked = 0
  var valueNotFound = 0
  var chunksSearchedAgain = 0
  var posListsChecked = 0
  val matches = mutableListOf<String>()

  if (keys.isNotEmpty()) {
    val genNamespacePrefix = namespacePrefixes[Namespace.GENERICS] ?: ""
    var i = searchBytes(buf, 0, buf.size, patterns[0], processedPatterns[0])
    while (i >= 0) {
      checked++
      var nextSearchPos = i + patterns[0].size

      var allValuesMatch = true
      var chunkRange: LongRange? = null
      for ((j, key) in keys.withIndex()) {
        val p = if (chunkRange != null) {
          // We already found a chunk (from searching for the first key). Search
          // inside the chunk again and try to find the next key.
          chunksSearchedAgain++
          searchBytes(buf, chunkRange.first, chunkRange.last, patterns[j], processedPatterns[j])
        } else {
          i
        }
        if (p == -1L) {
          allValuesMatch = false
          break
        }

        val actualValue = getAttributeValue(buf, p, genNamespacePrefix)
        if (actualValue == null) {
          valueNotFound++
          break
        }

        if (matcher(key, actualValue.text)) {
          chunkRange = findChunk(buf, actualValue.start, actualValue.end, namespacePrefixes)
        } else {
          allValuesMatch = false
          break
        }
      }

      if (chunkRange != null) {
        if (allValuesMatch) {
          val chunk = substring(buf, chunkRange.first, chunkRange.last)
          matches.add(chunk)
        }
        nextSearchPos = chunkRange.last
      }

      i = searchBytes(buf, nextSearchPos, buf.size, patterns[0], processedPatterns[0])
    }
  } else if (bbox != null) {
    // no keys - just look for bounding box
    val gmlNamespacePrefix = namespacePrefixes[Namespace.GML] ?: ""
    val posListPattern = "<${gmlNamespacePrefix}posList".toByteArray()
    val processedPosListPattern = processBytes(posListPattern)
    var i = searchBytes(buf, 0, buf.size, posListPattern, processedPosListPattern)
    while (i >= 0) {
      posListsChecked++
      var nextSearchPos = i + posListPattern.size

      val intersectsPos = intersectsPosList(buf, i, bbox)
      if (intersectsPos != -1L) {
        val chunkRange = findChunk(buf, i, intersectsPos, namespacePrefixes)
        val chunk = substring(buf, chunkRange.first, chunkRange.last)
        checked++
        matches.add(chunk)
        nextSearchPos = chunkRange.last
      }

      i = searchBytes(buf, nextSearchPos, buf.size, posListPattern, processedPosListPattern)
    }
  }

  val duration = System.currentTimeMillis() - start
  return SearchResult(matches = matches, filesize = buf.size, keys = keys, bbox = bbox,
      duration = duration, chunksChecked = checked, posListsChecked = posListsChecked,
      chunksSearchedAgain = chunksSearchedAgain, valueNotFound = valueNotFound)
}

// strategy: find 'value' in text, compare key, get chunk around it
fun runByValue(path: String, key: String?, value: String): SearchResult {
  log(path)

  val start = System.currentTimeMillis()

  val buf = FileBuffer(path)

  val rootElement = readRootElement(buf)
  val namespacePrefixes = extractNamespacePrefixes(rootElement)
  val genNamespacePrefix = namespacePrefixes[Namespace.GENERICS] ?: ""

  var checked = 0
  val pattern = value.toByteArray()
  val processedPattern = processBytes(pattern)
  var i = searchBytes(buf, 0, buf.size, pattern, processedPattern)
  val matches = mutableListOf<String>()
  while (i >= 0) {
    checked++
    var nextSearchPos = i + pattern.size

    val chunk = extractChunk(buf, i, i, namespacePrefixes)
    if (key == null || getAttributeKey(buf, i, genNamespacePrefix)?.text == key) {
      matches.add(chunk.text)
      nextSearchPos = chunk.end
    }

    i = searchBytes(buf, nextSearchPos, buf.size, pattern, processedPattern)
  }

  return SearchResult(matches, filesize = buf.size, keys = if (key != null) listOf(key) else emptyList(),
      values = listOf(value), duration = System.currentTimeMillis() - start,
      chunksChecked = checked)
}

@Suppress("BlockingMethodInNonBlockingContext")
suspend fun sync() = withContext(Dispatchers.IO) {
  val code1 = Runtime.getRuntime().exec("sync").waitFor()
  if (code1 != 0) {
    throw RuntimeException("Could not run `sync'. Exit code $code1")
  }
  val code2 = Runtime.getRuntime().exec("purge").waitFor()
  if (code2 != 0) {
    throw RuntimeException("Could not run `purge'. Exit code $code2. " +
        "`sudo' is required to run this application.")
  }
}

suspend fun benchmark(warmupRuns: Int = DEFAULT_WARMUP_RUNS, runs: Int = DEFAULT_BENCH_RUNS, block: suspend () -> Unit) {
  runType = "WARM"
  maxRuns = warmupRuns
  run = 0
  log("Warmup ...")
  val warmupStats = DescriptiveStatistics()
  for (i in 1..warmupRuns) {
    sync()
    run = i
    val s = System.currentTimeMillis()
    block()
    val e = System.currentTimeMillis()
    warmupStats.addValue((e - s).toDouble())
  }
  val warmupDuration = warmupStats.sum
  log("Warmup completed after $warmupDuration ms")

  runType = "BENCH"
  maxRuns = runs
  run = 0
  log("Run benchmark ...")
  val stats = DescriptiveStatistics()
  for (i in 1..runs) {
    sync()
    run = i
    val s = System.currentTimeMillis()
    block()
    val e = System.currentTimeMillis()
    stats.addValue((e - s).toDouble())
  }
  val duration = stats.sum

  runType = "WARM"
  maxRuns = warmupRuns
  run = warmupRuns
  log("-----")
  log("Warmup completed after $warmupDuration ms")
  log("Number of warmup runs: $warmupRuns")
  log("Average per warmup run: ${warmupStats.mean.roundToInt()} ms")
  log("Median per warmup run: ${warmupStats.getPercentile(0.5).roundToInt()} ms")
  log("Warmup min: ${warmupStats.min.roundToInt()} ms")
  log("Warmup max: ${warmupStats.max.roundToInt()} ms")
  log("Warmup std. dev.: ${warmupStats.standardDeviation.roundToInt()} ms")

  runType = "BENCH"
  maxRuns = runs
  run = runs
  log("-----")
  log("Benchmark completed after $duration ms")
  log("Number of runs: $runs")
  log("Average per run: ${stats.mean.roundToInt()} ms")
  log("Median per run: ${stats.getPercentile(0.5).roundToInt()} ms")
  log("Min: ${stats.min.roundToInt()} ms")
  log("Max: ${stats.max.roundToInt()} ms")
  log("Std. dev.: ${stats.standardDeviation.roundToInt()} ms")
  log("-----")
}

suspend fun singleOrMultiple(single: Boolean = true, block: suspend (path: String) -> SearchResult) = coroutineScope {
  val path = "/Users/mkraemer/code/ogc3dc/DA_WISE_GML_enhanced"
  val files = if (single) {
    listOf("DA12_3D_Buildings_Merged.gml")
  } else {
    File(path).list().toList()
  }

  val ds = files.map { f ->
    async {
      block("$path/$f")
    }
  }
  val results = ds.awaitAll()

  val size = results.sumOf { it.filesize.toInt() }
  val keys = results[0].keys
  val values = results[0].values
  val bbox = results[0].bbox
  val duration = results.sumOf { it.duration.toInt() }
  val checked = results.sumOf { it.chunksChecked }
  val nMatches = results.sumOf { it.matches.size }
  val posListsChecked = results.sumOf { it.posListsChecked }
  val chunksSearchedAgain = results.sumOf { it.chunksSearchedAgain }
  val valueNotFound = results.sumOf { it.valueNotFound }
  log("File size: $size")
  log("Keys: $keys")
  log("Values: $values")
  log("Bounding box: $bbox")
  log("Time taken: $duration")
  log("Chunks checked: $checked")
  log("Matches: $nMatches")
  log("Misses: ${checked - nMatches}")
  log("Pos lists checked: $posListsChecked")
  log("Times a chunk was searched again for another key-value pair: $chunksSearchedAgain")
  log("Places where 'key' was not a valid string attribute: $valueNotFound")
}

// TODO caveat: only supports plain ASCII encoding, proper UTF-8 decoding might actually affect performance
// TODO caveat: if file is not well formatted we might search until the end (e.g. when looking for the end of a tag or chunk)
suspend fun main() {
  val test = 1
  File(LOG_FILE).delete()

  // look for free text without key
  // should yield 1 chunk
  if (test == 1) {
    log("*** Free text")
    benchmark {
      singleOrMultiple { path ->
        runByValue(path, null, "Empire State Building")
      }
    }
  }

  // should yield 1 chunk
  if (test == 2) {
    log("*** Key & value (search by value)")
    benchmark {
      singleOrMultiple { path ->
        runByValue(path, "ownername", "Empire State Building")
      }
    }
  }

  if (test == 3) {
    log("*** Key & value")
    benchmark {
      singleOrMultiple { path ->
        run(path, "ownername", "Empire State Building")
      }
    }
  }

  // should yield 1179 chunks
  if (test == 4) {
    log("*** Zip code 10019 (search by value)")
    benchmark {
      singleOrMultiple { path ->
        runByValue(path, "zipcode", "10019")
      }
    }
  }

  if (test == 5) {
    log("*** Zip code 10019")
    benchmark {
      singleOrMultiple { path ->
        run(path, "zipcode", "10019")
      }
    }
  }

  if (test == 6) {
    log("*** Number of floors >= 4")
    benchmark {
      singleOrMultiple { path ->
        run(path, "numfloors") { v ->
          v.toDoubleOrNull()?.let { floors ->
            floors >= 4.0
          } ?: false
        }
      }
    }
  }

  if (test == 7) {
    log("*** Zip code range 10018..10020")
    benchmark {
      singleOrMultiple { path ->
        run(path, "zipcode") { v ->
          v.toIntOrNull()?.let { zipcode ->
            zipcode in 10018..10020
          } ?: false
        }
      }
    }
  }

  // bldgclass = Factory
  if (test == 8) {
    log("*** Building class `bldgclass` starts with `F` (Factory)")
    benchmark {
      singleOrMultiple { path ->
        run(path, "bldgclass") { it.startsWith("F") }
      }
    }
  }

  if (test == 11) {
    log("*** Bounding box")
    benchmark {
      singleOrMultiple { path ->
        run(path, emptyList(), BoundingBox(996800.0, 18600.0, 996900.0, 18700.0)) { _, _ -> true }
      }
    }
  }

  if (test == 12) {
    log("*** latitude AND longitude AND address")
    benchmark {
      singleOrMultiple { path ->
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
      }
    }
  }
}
