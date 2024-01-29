package com.mongodb.fastexport

import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import java.util.*

fun createTaxlot(): Document {
    return Document().apply {
        put("_id", "myId")
        put("firstName", "Bob")
        put("lastName", "Allen")
        put("date", Date())
        put("taxlots", (0..2).map {
            createTaxlotSubDoc(it)
        })
    }
}

fun createTaxlotSubDoc(idx: Int): Document =
    Document().apply {
        put("rec", idx)
        put("cost", idx)
        put("type", "type2")
        put("code", "B")
    }

fun dateQuery(date: String): String =
    Document().apply {
        put("date", Document("\$gte", Document("\$date", date)))
    }.toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build())
        .also {
            println("Date query: $it")
        }