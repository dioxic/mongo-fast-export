package com.mongodb.fastexport

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.output.MordantHelpFormatter
import com.github.ajalt.clikt.parameters.groups.OptionGroup
import com.github.ajalt.clikt.parameters.groups.defaultByName
import com.github.ajalt.clikt.parameters.groups.groupChoice
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import okio.FileSystem
import okio.Path.Companion.toOkioPath
import okio.Path.Companion.toPath
import okio.buffer
import okio.sink
import java.time.format.DateTimeFormatter
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

sealed class Mode(val fileExtension: String, name: String, help: String) : OptionGroup(name, help)
class JsonOptionGroup : Mode("json", "Json Mode", "JSON export mode")
class CsvOptionGroup : Mode("csv", "CSV Mode", "CSV export mode") {
    val arrayField by option("-a", "--array", help = "array field")
    val dateFormatter: DateTimeFormatter by option("--date-format", help = "date format pattern").convert {
        DateTimeFormatter.ofPattern(it)
    }.default(DateTimeFormatter.ISO_DATE_TIME)
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

    private val query by option("-q", "--query", help = "filter query (in MQL)")

    private val fields by option("-f", "--fields", help = "fields to include (comma-delimited)")
        .split(",")

    private val mode by option("--mode", help = "export mode").groupChoice(
        "json" to JsonOptionGroup(),
        "csv" to CsvOptionGroup()
    ).defaultByName("csv")

    private val path by option("-o", "--output", help = "output filename").path(canBeDir = false).convert {
        it.toOkioPath()
    }

    private val test by option("--test", help = "print 5 record to the console for testing").flag()

    override fun run() {
        if (mode is CsvOptionGroup && fields == null) {
            throw UsageError("missing option --fields must be specified in CSV export mode", "fields")
        }

        echo("connecting to $uri...")

        val client = createClient(uri)

        val collectionSize = client
            .getDatabase(database)
            .getCollection(collection)
            .estimatedDocumentCount()
            .also {
                if (it == 0L) {
                    echo("$database.$collection is empty or does not exist!")
                    return
                }
            }

        query?.also { echo("filter query: $it") }
        fields?.also { echo( "projection: ${it.toProjection()?.toBsonDocument()?.toJson()}") }

        val sink = when {
            test -> {
                echo("exporting '$database.$collection' to console")
                System.out.sink().buffer()
            }
            else -> {
                val p = path ?: "$collection.${mode.fileExtension}".toPath()
                echo("exporting '$database.$collection' to ${p.name}")
                FileSystem.SYSTEM.sink(p).buffer()
            }
        }

        val limit = when {
            test -> 5
            else -> null
        }

        measureTime {
            runBlocking(Dispatchers.Default) {
                when (val it = mode) {
                    is JsonOptionGroup -> jsonExport(
                        client = client,
                        database = database,
                        collection = collection,
                        fields = fields,
                        filter = query.toQueryBson(),
                        limit = limit,
                        sink = sink
                    )

                    is CsvOptionGroup -> csvExport(
                        client = client,
                        database = database,
                        collection = collection,
                        fields = fields!!,
                        filter = query.toQueryBson(),
                        limit = limit,
                        arrayField = it.arrayField,
                        sink = sink,
                        dateFormatter = it.dateFormatter
                    )
                }
            }
        }.also { duration ->
            echo("Completed export of $database.$collection in $duration")
        }
    }
}

fun main(args: Array<String>) = Cli().main(args)