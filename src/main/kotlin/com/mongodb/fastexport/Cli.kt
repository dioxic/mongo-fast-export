package com.mongodb.fastexport

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.output.MordantHelpFormatter
import com.github.ajalt.clikt.parameters.groups.OptionGroup
import com.github.ajalt.clikt.parameters.groups.defaultByName
import com.github.ajalt.clikt.parameters.groups.groupChoice
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.runningFold
import kotlinx.coroutines.runBlocking
import okio.Buffer
import okio.FileSystem
import okio.Path.Companion.toOkioPath
import okio.Path.Companion.toPath
import okio.buffer
import okio.sink
import org.bson.Document
import org.bson.json.JsonMode
import java.time.format.DateTimeFormatter
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

sealed class Mode(val fileExtension: String, name: String, help: String) : OptionGroup(name, help)
class JsonOptionGroup : Mode("json", "Json Mode", "JSON export mode") {
    val jsonFormat by option("--json-format", help = "json format to export").enum<JsonMode>()
        .default(JsonMode.RELAXED)
}
class CsvOptionGroup : Mode("csv", "CSV Mode", "CSV export mode") {
    val arrayField by option("-a", "--array", help = "array field to denormalize")
    val dateFormatter: DateTimeFormatter by option("--date-format", help = "date format pattern").convert {
        DateTimeFormatter.ofPattern(it)
    }.default(DateTimeFormatter.ISO_DATE_TIME, defaultForHelp = "ISO Format")
    val includeHeader by option("--header", help = "include csv header in export").flag()
    val delimiter by option("--delimiter", help = "delimiter character").default(",")
}

class Cli : CliktCommand() {
    init {
        context {
            helpFormatter = { MordantHelpFormatter(it, showDefaultValues = true, requiredOptionMarker = "*") }
        }
    }

    private val uri: String by option("--uri", help = "MongoDB connection string").required()
        .validate {
            require(
                it.startsWith("mongodb://")
                        || it.startsWith("mongodb+srv://")
                        || it.startsWith("msmongodb://")
            ) {
                "Invalid uri expecting mongodb:// or mongodb+srv:// or msmongodb://"
            }
        }

    private val database by option("-d", "--database", help = "database name").required()

    private val collection by option("-c", "--collection", help = "collection name").required()

    private val query by option("-q", "--query", help = "filter query (in MQL)").convert {
        Document.parse(it)
    }.default(Document(), defaultForHelp = "{}")

    private val fields by option("-f", "--fields", help = "fields to include (comma-delimited)")
        .split(",")

    private val projection by option("--projection", help = "projection (in MQL)").convert {
        Document.parse(it)
    }

    private val mode by option("--mode", help = "export mode").groupChoice(
        "json" to JsonOptionGroup(),
        "csv" to CsvOptionGroup()
    ).defaultByName("csv")

    private val path by option("-o", "--output", help = "output filename").path(canBeDir = false).convert {
        it.toOkioPath()
    }

    private val outputTick by option("--outputTickSeconds", hidden = true).int().default(1)

    private val testFlag by option("--test", help = "print a single document to the console for testing").flag()

    private val limitArg by option("--limit", help="limit the number of documents to export").int()

    override fun run() {
        if (mode is CsvOptionGroup && fields == null && projection == null) {
            throw UsageError("--fields or --projection must be specified in CSV export mode.", "fields")
        }

        if (projection != null && fields != null) {
            throw UsageError("--projection and --fields cannot both be specified.")
        }

        query?.also { echo("filter query: ${it.toJson()}") }
        fields?.also { echo("projection: ${it.toProjection()?.toBsonDocument()?.toJson()}") }
        projection?.also { echo("projection: ${it.toBsonDocument().toJson()}") }

        echo("connecting to $uri...")

        val client = createClient(uri)

        client
            .getDatabase(database)
            .getCollection(collection)
            .estimatedDocumentCount()
            .also {
                if (it == 0L) {
                    echo("$database.$collection is empty or does not exist!")
                    return
                }
            }

        val bufferedSink = when {
            testFlag -> {
                echo("exporting '$database.$collection' to console")
                echo("----------------------------------------------")
                Buffer()
            }

            else -> {
                val p = path ?: "$collection.${mode.fileExtension}".toPath()
                echo("exporting '$database.$collection' to ${p.name}")
                FileSystem.SYSTEM.sink(p).buffer()
            }
        }

        val limit = when {
            testFlag -> limitArg ?: 1
            else -> limitArg
        }

        measureTime {
            runBlocking(Dispatchers.Default) {
                bufferedSink.use { sink ->
                    when (val modeGrp = mode) {
                        is JsonOptionGroup -> sink.jsonExport(
                            client = client,
                            database = database,
                            collection = collection,
                            projection = projection ?: fields.toProjection(),
                            filter = query,
                            limit = limit,
                            jsonFormat = modeGrp.jsonFormat,
                        )

                        is CsvOptionGroup -> sink.csvExport(
                            client = client,
                            database = database,
                            collection = collection,
                            projection = projection ?: fields.toProjection()!!,
                            filter = query,
                            limit = limit,
                            arrayField = modeGrp.arrayField,
                            dateFormatter = modeGrp.dateFormatter,
                            delimiter = modeGrp.delimiter,
                            includeHeader = modeGrp.includeHeader
                        )
                    }.runningFold(0L) { accumulator, _ ->
                        accumulator + 1L
                    }.conflate()
                        .onEach { delay(outputTick.seconds) }
                        .collect {
                            if (!testFlag) {
                                echo("exported $it records")
                            }
                        }
                }
            }
        }.also { duration ->
            if (testFlag) {
                echo((bufferedSink as Buffer).readByteString().utf8())
            }
            else {
                echo("Completed export of '$database.$collection' in $duration")
            }
        }
    }
}

fun main(args: Array<String>) = Cli().main(args)