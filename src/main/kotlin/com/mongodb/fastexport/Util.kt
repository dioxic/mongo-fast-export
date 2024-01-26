package com.mongodb.fastexport

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
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