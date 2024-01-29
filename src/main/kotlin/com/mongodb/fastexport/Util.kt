package com.mongodb.fastexport

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import org.bson.BsonDocument
import org.bson.BsonValue
import org.bson.Document
import org.bson.conversions.Bson
import org.slf4j.Logger
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

fun String?.toQueryBson(): Bson =
    this?.let { Document.parse(it) } ?: Filters.empty()

fun List<String>?.toProjection2(): Bson? =
    this?.let {
        Projections.fields(it.map { field ->
            Projections.computed(field, "\$$field")
        })
    }.also {
        println("projection: ${it?.toBsonDocument()?.toJson()}")
    }

fun List<String>?.toProjection(): Bson? =
    this?.let {
        Projections.include(this)
    }.also {
        println("projection: ${it?.toBsonDocument()?.toJson()}")
    }

fun Map<String, Any?>.flatten(separator: Char = '.', leafOnly: Boolean = false) =
    mutableMapOf<String, Any?>().also {
        flatten(it, this, separator, leafOnly)
    }.toMap()

fun BsonDocument.flatten(separator: Char = '.', leafOnly: Boolean = false) =
    mutableMapOf<String, BsonValue>().also {
        flatten(it, this, separator, leafOnly)
    }.toMap()

@Suppress("UNCHECKED_CAST")
private fun <T> flatten(
    map: MutableMap<String, T>,
    value: T,
    separator: Char,
    leafOnly: Boolean,
    key: String = ""
) {
    when (value) {
        is Map<*, *> -> {
            value.filterValues { it != null }.forEach { (k, v) ->
                val newKey = getKey(key, separator, k.toString())
                flatten(map, v as T, separator, leafOnly, newKey)
            }
            if (!leafOnly && key.isNotEmpty()) {
                map[key] = value
            }
        }

        is Iterable<*> -> {
            value.filterNotNull().forEachIndexed { i, v ->
                val newKey = getKey(key, separator, i.toString())
                flatten(map, v as T, separator, leafOnly, newKey)
            }
            if (!leafOnly) {
                map[key] = value
            }
        }

        else -> map[key] = value
    }
}

private fun getKey(prefix: String, separator: Char, suffix: String) =
    if (prefix.isEmpty()) {
        suffix
    } else {
        "$prefix$separator$suffix"
    }