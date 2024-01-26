package com.mongodb.fastexport

import com.mongodb.MongoNamespace
import com.mongodb.client.MongoClient
import com.mongodb.fastexport.NsFilter.Exclude
import com.mongodb.fastexport.NsFilter.Include

sealed interface NsFilter {
    val filter: MongoNamespace

    data class Include(override val filter: MongoNamespace) : NsFilter
    data class Exclude(override val filter: MongoNamespace) : NsFilter
}

fun List<NsFilter>.match(namespace: MongoNamespace): Boolean =
    when (first()) {
        is Include -> any { match(namespace, it.filter) }
        is Exclude -> none { match(namespace, it.filter) }
    }

fun List<NsFilter>.listNamespaces(client: MongoClient): List<MongoNamespace> =
    map { ns -> client.getDatabase(ns.filter.databaseName) }
        .flatMap { db -> db.listCollectionNames().map { MongoNamespace("${db.name}.$it") } }
        .filter { match(it) }

fun match(ns: MongoNamespace, filter: MongoNamespace): Boolean =
    ((ns.databaseName == filter.databaseName || filter.databaseName == "*")
            && (ns.collectionName == filter.collectionName || filter.collectionName == "*"))