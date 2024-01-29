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

    private val nsFilters: List<NsFilter>? by mutuallyExclusiveOptions(
        option("--nsInclude", help = "namespaces to include").convert { Include(MongoNamespace(it)) }.multiple(),
        option("--nsExclude", help = "namespaces to exclude").convert { Exclude(MongoNamespace(it)) }.multiple()
    ).single()
//        .default(
//        listOf(
//            Exclude(MongoNamespace("admin.*")),
//            Exclude(MongoNamespace("local.*")),
//            Exclude(MongoNamespace("config.*"))
//        )
//    )

    private val fields by option("-f", "--fields", help = "files to include (comma-delimited)")
        .split(",")

    private val query by option("-q", "--query", help = "query to apply (in MQL)")

    override fun run() {
        echo("Namespace filters: $nsFilters")

        val jws = JsonWriterSettings.builder().indent(true).build()

        echo("connecting to $uri...")

        val mcs = MongoClientSettings.builder().applyToClusterSettings {
            it.serverSelectionTimeout(3L, TimeUnit.SECONDS)
        }.applyConnectionString(uri).build()

        val client = uri.createClient()

//        val namespaces = (database?.let { listOf(it) }
//            ?: client.listDatabaseNames()).flatMap { dbName ->
//            val db = client.getDatabase(dbName)
//            db.listCollectionNames().map { MongoNamespace("$dbName.$it") }
//        }.filter { nsFilters.match(it) }

        val namespaces = nsFilters?.listNamespaces(client) ?: client.listNamespaces()

        namespaces.forEach { ns ->
            echo("processing namespace $ns")
            val queryBson = query?.let { Document.parse(it) } ?: Filters.empty()
            client.getDatabase(ns.databaseName).getCollection(ns.collectionName)
                .find(queryBson)
                .projection(fields?.let { Projections.include(it) })
                .count()
                .also {
                    echo("count: $it")
                }
//                .forEach {
//                    echo(it.toJson(jws))
//                }
        }

        echo("filtered: $namespaces")

    }

}

fun main(args: Array<String>) = Cli().main(args)