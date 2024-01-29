package com.mongodb.fastexport

import arrow.fx.coroutines.parMap
import com.mongodb.client.MongoClient
import com.mongodb.client.model.Aggregates
import okio.BufferedSink
import org.bson.Document
import org.bson.RawBsonDocument
import org.bson.codecs.EncoderContext
import org.bson.codecs.RawBsonDocumentCodec
import org.bson.conversions.Bson
import org.bson.json.JsonMode
import org.bson.json.JsonWriter
import org.bson.json.JsonWriterSettings
import java.io.StringWriter

private val jws: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()
private val codec = RawBsonDocumentCodec()
private val context: EncoderContext = EncoderContext.builder().build()

suspend fun jsonExport(
    client: MongoClient,
    database: String,
    collection: String,
    fields: List<String>?,
    filter: Bson,
    sink: BufferedSink,
) {

    sink.use { bs ->
        client
            .getDatabase(database)
            .getCollection(collection, RawBsonDocument::class.java)
            .find(filter)
            .projection(fields.toProjection())
            .parMap { doc ->
                val writer = StringWriter()
                codec.encode(JsonWriter(writer, jws), doc, context)
                bs.writeUtf8(writer.toString())
                bs.writeUtf8("\n")
            }
    }
}

suspend fun csvExport(
    client: MongoClient,
    database: String,
    collection: String,
    fields: List<String>,
    filter: Bson,
    sink: BufferedSink,
    arrayField: String?
) {
    val pipeline = buildList<Bson> {
        add(Aggregates.match(filter))
        arrayField?.also { add(Aggregates.unwind("\$$it")) }
        add(Aggregates.project(fields.toProjection()!!))
    }

    sink.use { bs ->
        client
            .getDatabase(database)
            .getCollection(collection, Document::class.java)
            .aggregate(pipeline)
            .parMap { doc ->
                val flatDoc = doc.flatten(leafOnly = true)
                bs.writeUtf8(fields.map { flatDoc[it] }.joinToString(","))
                bs.writeUtf8("\n")
            }
    }
}

//val JsonTransformer: (BufferedSink, RawBsonDocument) -> Unit = { sink, doc ->
//    val writer = StringWriter()
//    codec.encode(JsonWriter(writer, jws), doc, context)
//    sink.writeUtf8(writer.toString())
//    sink.writeUtf8("\n")
//}
//
//val CsvTransformer: (BufferedSink, RawBsonDocument) -> Unit = { sink, doc ->
//    val writer = StringWriter()
//    codec.encode(CsvWriter(writer), doc, context)
//    sink.writeUtf8(writer.toString())
//    sink.writeUtf8("\n")
//}