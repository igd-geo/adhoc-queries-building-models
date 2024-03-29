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
import java.io.Serializable
import java.io.StringReader
import kotlin.math.roundToInt

val XML_FACTORY = SMInputFactory(InputFactoryImpl())

const val DEFAULT_WARMUP_RUNS = 2
const val DEFAULT_BENCH_RUNS = 20

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

var disableLog = false
var runType = "BENCH"
var run = 0
var maxRuns = 10
const val LOG_FILE = "out.log"

val indexUseStats = DescriptiveStatistics()
val indexBuildStats = DescriptiveStatistics()
val adhocSearchStats = DescriptiveStatistics()

fun log(msg: String) {
  if (disableLog) return
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
data class BoundingBox(val minX: Double, val minY: Double, val maxX: Double, val maxY: Double): Serializable {
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

/**
 * Get the tag name of the first tag.
 * XXX <hello>world</hello>
 * returns "hello"
 */
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

/**
 * Get a map of the namespaces in Namespace.* to the URI defined in the [rootElement]
 */
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

/**
 * @return true if the bbox contains all points of the poslist
 * that starts at position i in buf. false otherwise.
 * Together with the last checked byte position.
 */
fun containsPosList(buf: FileBuffer, i: Long, bbox: BoundingBox): Pair<Boolean, Long> {
  val posListStart = indexOf(buf, i, ">") + 1

  val sb = StringBuilder()
  var j = posListStart
  var n = 0
  var x = 0.0
  var y = 0.0

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
          x = sb.toString().toDouble()
          if (x !in bbox.minX..bbox.maxX) return Pair(false, j)
          n++
        }
        1 -> {
          y = sb.toString().toDouble()
          if (y !in bbox.minY..bbox.maxY) return Pair(false, j)
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

  return Pair(true, j)
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

fun isChunkInBoundingBox(chunkRange: LongRange, buf: FileBuffer, firstPosListThatIsInBoundingBox: LongRange, bbox: BoundingBox,
                         posListPattern: ByteArray, processedPosListPattern: IntArray): Boolean {

  val firstPosListInChunk = searchBytes(buf, chunkRange.first, chunkRange.last, posListPattern, processedPosListPattern)

  // There must not be a posList in the chunk before the found one (starting at firstPosListThatIsInBoundingBox)
  // Otherwise this would have been found before if its coordinates are in the bbox.
  if (firstPosListInChunk < firstPosListThatIsInBoundingBox.first) {
    return false
  }

  var nextPosList = searchBytes(buf, firstPosListThatIsInBoundingBox.last, chunkRange.last, posListPattern, processedPosListPattern)
  while (nextPosList >= 0) {
    val containsPosList = containsPosList(buf, nextPosList, bbox)
    if (!containsPosList.first) return false // This posList is not in the bbox --> The chunk is not in the bbox
    nextPosList = searchBytes(buf, containsPosList.second, chunkRange.last, posListPattern, processedPosListPattern)
  }

  return true
}

// strategy find 'key' in text, check if it's an attribute, get attribute
// value, compare with 'value', finally get complete chunk if value matches
fun run(path: String, key: String, value: String): SearchResult {
  return run(path, key) { it.contains(value) }
}

fun run(path: String, key: String, matches: (String) -> Boolean): SearchResult {
  return run(path, listOf(key)) { _, value -> matches(value) }
}

fun run(path: String, keys: List<String>, bbox: BoundingBox? = null, useIndex: Boolean = false,
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

    var indexer: Indexer? = null
    var adhocStart = 0L
    if (useIndex) {
      val indexUseStart = System.currentTimeMillis()
      indexer = Indexer(path)
      val indexHits = indexer.find(bbox)
      indexUseStats.addValue((System.currentTimeMillis() - indexUseStart).toDouble())
      matches.addAll(indexHits)
      adhocStart = indexer.index?.indexedBoundary ?: 0
    }

    val adhocSearchStart = System.currentTimeMillis()
    val gmlNamespacePrefix = namespacePrefixes[Namespace.GML] ?: ""
    val posListPattern = "<${gmlNamespacePrefix}posList".toByteArray()
    val processedPosListPattern = processBytes(posListPattern)

    // Get the byte position of the first hit
    var i = searchBytes(buf, adhocStart, buf.size, posListPattern, processedPosListPattern)
    while (i >= 0) {
      posListsChecked++
      var nextSearchPos = i + posListPattern.size

      val containsPosList = containsPosList(buf, i, bbox)
      if (containsPosList.first) {
        // This posList is in the searched bounding box
        // --> The corresponding chunk might be a result
        // --> Check the other posLists of this chunk
        val chunkRange = findChunk(buf, i, containsPosList.second, namespacePrefixes)
        checked++
        val chunkValid = isChunkInBoundingBox(chunkRange, buf, i..containsPosList.second, bbox, posListPattern, processedPosListPattern)
        if (chunkValid) {
          val chunk = substring(buf, chunkRange.first, chunkRange.last)
          matches.add(chunk)
        }
        nextSearchPos = chunkRange.last
      } else {
        // We can continue at the last checked position of the posList. The bytes before only contain coordinates.
        nextSearchPos = containsPosList.second
      }

      i = searchBytes(buf, nextSearchPos, buf.size, posListPattern, processedPosListPattern)
    }
    adhocSearchStats.addValue((System.currentTimeMillis() - adhocSearchStart).toDouble())

    if (useIndex) {
      val indexBuildStart = System.currentTimeMillis()
      val remainingTime = 500 - (indexBuildStart - start)
      if (remainingTime < 0) indexer?.index(2000) // There is no time left -> Index only a few chunks
      else indexer?.index(remainingTime.toInt() * 500)
      indexer?.saveIndex()
      indexBuildStats.addValue((System.currentTimeMillis() - indexBuildStart).toDouble())
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

/**
 * Clear cache.
 */
@Suppress("BlockingMethodInNonBlockingContext")
suspend fun sync() = withContext(Dispatchers.IO) {
  val code1 = Runtime.getRuntime().exec("sync").waitFor()
  if (code1 != 0) {
    throw RuntimeException("Could not run `sync'. Exit code $code1")
  }
  val code2 = if (System.getProperty("os.name") == "Linux") {
    Runtime.getRuntime().exec(arrayOf("/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches")).waitFor()
  } else {
    Runtime.getRuntime().exec("purge").waitFor()
  }
  if (code2 != 0) {
    throw RuntimeException("Could not run `purge'. Exit code $code2. " +
        "`sudo' is required to run this application.")
  }
}

/**
 * Runs [warmupRuns] times the [block] for warmup.
 * Afterwards [runs] times for benchmarking.
 * Clears cache between each run.
 */
suspend fun benchmark(warmupRuns: Int = DEFAULT_WARMUP_RUNS, runs: Int = DEFAULT_BENCH_RUNS, preRunHook: () -> Unit, block: suspend () -> Unit) {
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

  preRunHook()

  indexUseStats.clear()
  indexBuildStats.clear()
  adhocSearchStats.clear()

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
  log("All total times: ${stats.values.joinToString(" ")}")
  log("All adhocSearchStats: ${adhocSearchStats.values.joinToString(" ")}")
  log("All indexBuildStats: ${indexBuildStats.values.joinToString(" ")}")
  log("All indexUseStats: ${indexUseStats.values.joinToString(" ")}")
  log("-----")
}

/**
 * Runs [block] for each GML file in [files] in parallel.
 */
suspend fun singleOrMultiple(files: List<String>, block: suspend (path: String) -> SearchResult) = coroutineScope {
  val ds = files.map { f ->
    async {
      block(f)
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

  return@coroutineScope results
}

// TODO caveat: only supports plain ASCII encoding, proper UTF-8 decoding might actually affect performance
// TODO caveat: if file is not well formatted we might search until the end (e.g. when looking for the end of a tag or chunk)
suspend fun runTest(test: Int, files: List<String>, benchmark: Boolean) {
  File(LOG_FILE).delete()

  val testBlock: Pair<String, suspend (String) -> SearchResult> = when (test) {
    1 -> Pair("*** Free text") { path ->
      // look for free text without key
      // should yield 1 chunk
      runByValue(path, null, "Empire State Building")
    }
    2 -> Pair("*** Key & value (search by value)") { path ->
      // should yield 1 chunk
      runByValue(path, "ownername", "Empire State Building")
    }
    3 -> Pair("*** Key & value") { path ->
      run(path, "ownername", "Empire State Building")
    }
    4 -> Pair("*** Zip code 10019 (search by value)") { path ->
      // should yield 1179 chunks
      runByValue(path, "zipcode", "10019")
    }
    5 -> Pair("*** Zip code 10019") { path ->
      run(path, "zipcode", "10019")
    }
    6 -> Pair("*** Number of floors >= 4") { path ->
      run(path, "numfloors") { v ->
        v.toDoubleOrNull()?.let { floors ->
          floors >= 4.0
        } ?: false
      }
    }
    7 -> Pair("*** Zip code range 10018..10020") { path ->
      run(path, "zipcode") { v ->
        v.toIntOrNull()?.let { zipcode ->
          zipcode in 10018..10020
        } ?: false
      }
    }
    8 -> Pair("*** Building class `bldgclass` starts with `F` (Factory)") { path ->
      // bldgclass = Factory
      run(path, "bldgclass") { it.startsWith("F") }
    }
    9 -> Pair("*** Bounding box") { path ->
      run(path, emptyList(), BoundingBox(987700.0, 211100.0, 987900.0, 211300.0)) { _, _ -> true }
    }
    10 -> Pair("*** zipcode AND numfloors") { path ->
      run(path, listOf("zipcode", "numfloors")) { k, v ->
        if (k == "zipcode") {
          v.toIntOrNull()?.let { zipcode ->
            zipcode in 10018..10020
          } ?: false
        } else {
          v.toDoubleOrNull()?.let { floors ->
            floors >= 4.0
          } ?: false
        }
      }
    }
    11 -> Pair("*** numfloors AND zipcode") { path ->
      run(path, listOf("numfloors", "zipcode")) { k, v ->
        if (k == "zipcode") {
          v.toIntOrNull()?.let { zipcode ->
            zipcode in 10018..10020
          } ?: false
        } else {
          v.toDoubleOrNull()?.let { floors ->
            floors >= 4.0
          } ?: false
        }
      }
    }
    12 -> Pair("*** Bounding box (large)") { path ->
      run(path, emptyList(), BoundingBox(950000.0, 210000.0, 1000000.0, 220000.0)) { _, _ -> true }
    }
    13 -> Pair("*** Bounding box (like test 9) with index") { path ->
      run(path, emptyList(), BoundingBox(987700.0, 211100.0, 987900.0, 211300.0), true) { _, _ -> true }
    }
    14 -> Pair("*** Bounding box (large) (like test 12) with index") { path ->
      run(path, emptyList(), BoundingBox(950000.0, 210000.0, 1000000.0, 220000.0), true) { _, _ -> true }
    }
    else -> throw RuntimeException("Unknown test $test. Set a value between 1 and 13.")
  }

  if (benchmark) {
    log(testBlock.first) // The "title" of the test.

    // Delete index files from warm up.
    val preRunHook = { ->
      files
        .map { File("$it.index") }
        .filter { it.exists() }
        .forEach { it.delete() }
    }

    benchmark(preRunHook = preRunHook) {
      singleOrMultiple(files, testBlock.second)
    }
  } else {
    disableLog = true // We do not need log messages when only looking for the results.
    singleOrMultiple(files, testBlock.second)
      .flatMap { it.matches }
      .forEach { println(it) }
  }
}
