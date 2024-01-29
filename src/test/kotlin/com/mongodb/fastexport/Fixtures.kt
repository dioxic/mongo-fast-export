package com.mongodb.fastexport

import org.bson.Document
import org.bson.types.ObjectId
import java.util.*

fun createTaxlot(): Document {
    return Document().apply {
        put("_id", ObjectId("65b78d05d12faf22a9edaeaf"))
        put("firstName", "Bob")
        put("lastName", "Allen")
        put("date", Date())
        put("taxlots", (0..2).map {
            createTaxlotSubDoc()
        })
    }
}

fun createTaxlotSubDoc(): Document =
    Document().apply {
        put("rec", 111)
        put("cost", 222)
        put("type", "type2")
        put("code", "B")
    }