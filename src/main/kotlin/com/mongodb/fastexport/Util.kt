package com.mongodb.fastexport

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Projections.*
import org.bson.Document
import org.bson.conversions.Bson
import java.util.concurrent.TimeUnit

fun createClient(uri: String, serverSelectionTimeout: Long = 3): MongoClient =
    when {
        uri.startsWith("mongodb") -> {
            MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(uri))
                .applyToClusterSettings { it.serverSelectionTimeout(serverSelectionTimeout, TimeUnit.SECONDS) }
                .build()
                .let { mcs -> MongoClients.create(mcs) }
        }

        uri.startsWith("msmongodb") -> TODO("not supported yet")
        else -> error("Unknown protocol")
    }

fun String?.toQueryBson(): Document =
    this?.let { Document.parse(it) } ?: Document()

fun List<String>?.toProjection(): Bson? =
    this?.let {
        buildList<Bson> {
            if ("_id" !in it) {
                add(excludeId())
            }
            add(include(this@toProjection))
        }.let {
            fields(it)
        }
    }

fun Map<String, Any?>.flatten(separator: Char = '.', leafOnly: Boolean = false) =
    mutableMapOf<String, Any?>().also {
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