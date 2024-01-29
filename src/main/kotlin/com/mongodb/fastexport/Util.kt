package com.mongodb.fastexport

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import org.bson.Document
import java.util.concurrent.TimeUnit

fun MongoClient.listNamespaces(): List<MongoNamespace> =
    listDatabaseNames().flatMap { dbName ->
        getDatabase(dbName).listCollectionNames().map { MongoNamespace("$dbName.$it") }
    }

fun ConnectionString.createClient(serverSelectionTimeout: Long = 3): MongoClient =
    MongoClientSettings.builder()
        .applyConnectionString(this)
        .applyToClusterSettings { it.serverSelectionTimeout(serverSelectionTimeout, TimeUnit.SECONDS) }
        .build()
        .let { mcs ->
            when {
                connectionString.startsWith("mongodb") -> MongoClients.create(mcs)
                connectionString.startsWith("msmongodb") -> TODO("not supported yet")
                else -> error("Unknown protocol")
            }
        }

fun String?.toQueryBson() =
    this?.let { Document.parse(it) } ?: Filters.empty()

fun List<String>?.toProjection() =
    this?.let { Projections.include(it) }