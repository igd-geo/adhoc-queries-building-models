import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import kotlinx.coroutines.*
import java.io.File

class Main: CliktCommand() {
    private val test: Int by option(help="Run a predefined test. Number in range XX to YY (inclusive)").int().required()
    private val input: String by option(help="The input GML files to be searched. Can be a single file or directory").required()
    private val benchmark: Boolean by option(help="Set this flag if you want to run the test multiple times and get run time statistics.").flag(default = false)

    override fun run() {
        val files = if (File(input).isDirectory) {
            File(input).listFiles()!!.map { it.absolutePath }
        } else {
            listOf(input)
        }

        runBlocking(Dispatchers.Default) {
            runTest(test, files, benchmark)
        }
    }
}

fun main(args: Array<String>) = Main().main(args)
