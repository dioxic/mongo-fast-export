package com.mongodb.fastexport

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.groups.default
import com.github.ajalt.clikt.parameters.groups.mutuallyExclusiveOptions
import com.github.ajalt.clikt.parameters.groups.single
import com.github.ajalt.clikt.parameters.options.*
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import com.mongodb.fastexport.NsFilter.Exclude
import com.mongodb.fastexport.NsFilter.Include
import org.bson.Document
import org.bson.json.JsonWriterSettings
import java.util.concurrent.TimeUnit

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

    private val fields by option("-f", "--fields", help = "files to include (comma-delimited)")
        .split(",")

    private val query by option("-q", "--query", help = "query to apply (in MQL)")

    override fun run() {
        val jws = JsonWriterSettings.builder().indent(true).build()

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

        client
            .getDatabase(database)
            .getCollection(collection)
            .find(query.toQueryBson())
            .projection(fields.toProjection())
            .limit(1) // TODO remove this
            .forEach {
                echo(it.toJson(jws))
            }

    }

}

fun main(args: Array<String>) = Cli().main(args)