package com.mongodb.fastexport

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.groups.OptionGroup
import com.github.ajalt.clikt.parameters.groups.defaultByName
import com.github.ajalt.clikt.parameters.groups.groupChoice
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.path
import com.mongodb.ConnectionString
import kotlinx.coroutines.runBlocking
import okio.FileSystem
import okio.Path.Companion.toOkioPath
import okio.Path.Companion.toPath
import okio.buffer

sealed class Mode(name: String) : OptionGroup(name)
class JsonMode : Mode("json") {
    val fields by option("-f", "--fields", help = "files to include (comma-delimited)")
        .split(",")
}
class CsvMode : Mode("csv") {
    val arrayField by option("-a", "--array", help = "array field")
    val fields by option("-f", "--fields", help = "files to include (comma-delimited)")
        .split(",").required()
}

class Cli : CliktCommand() {

    private val uri: ConnectionString by option("--uri").convert { ConnectionString(it) }.required()
        .validate {
            with(it.connectionString) {
                require(startsWith("mongodb://") || startsWith("msmongodb://")) {
                    "Invalid uri expecting mongodb:// or msmongodb://"
                }
            }
        }

    private val database by option("-d", "--database", help = "database name").required()

    private val collection by option("-c", "--collection", help = "collection name").required()

    private val query by option("-q", "--query", help = "query to apply (in MQL)")

    private val mode by option().groupChoice(
        "json" to JsonMode(),
        "csv" to CsvMode()
    ).defaultByName("csv")
//    private val mode by option("--mode", help = "export mode").enum<ExportMode>().default(ExportMode.CSV)

    private val path by option("-o", "--output", help = "output filename").path(canBeDir = false).convert {
        it.toOkioPath()
    }

    override fun run() {
        echo("connecting to $uri...")

        val client = uri.createClient()

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

        val sink = FileSystem.SYSTEM.sink(path ?: "$collection.${mode.groupName?.lowercase()}".toPath()).buffer()

        runBlocking {
            when (val it = mode) {
                is JsonMode -> jsonExport(
                    client = client,
                    database = database,
                    collection = collection,
                    fields = it.fields,
                    filter = query.toQueryBson(),
                    sink = sink
                )

                is CsvMode -> csvExport(
                    client = client,
                    database = database,
                    collection = collection,
                    fields = it.fields,
                    filter = query.toQueryBson(),
                    arrayField = it.arrayField,
                    sink = sink
                )
            }
        }
    }
}

fun main(args: Array<String>) = Cli().main(args)