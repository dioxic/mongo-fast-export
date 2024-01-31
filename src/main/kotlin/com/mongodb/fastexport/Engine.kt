package com.mongodb.fastexport

import arrow.fx.coroutines.parMapUnordered
import com.mongodb.client.MongoClient
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Aggregates.limit
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import okio.BufferedSink
import org.bson.BsonInt32
import org.bson.RawBsonDocument
import org.bson.codecs.EncoderContext
import org.bson.codecs.RawBsonDocumentCodec
import org.bson.conversions.Bson
import org.bson.json.JsonMode
import org.bson.json.JsonWriter
import org.bson.json.JsonWriterSettings
import java.io.StringWriter
import java.time.format.DateTimeFormatter

@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
fun BufferedSink.jsonExport(
    client: MongoClient,
    database: String,
    collection: String,
    projection: Bson?,
    filter: Bson,
    limit: Int? = null,
    jsonFormat: JsonMode
): Flow<Unit> {
    val jws: JsonWriterSettings = JsonWriterSettings.builder().outputMode(jsonFormat).build()
    val codec = RawBsonDocumentCodec()
    val context: EncoderContext = EncoderContext.builder().build()

    return client
        .getDatabase(database)
        .getCollection(collection, RawBsonDocument::class.java)
        .find(filter)
        .let { cursor ->
            limit?.let { cursor.limit(limit) } ?: cursor
        }
        .projection(projection)
        .asFlow()
        .parMapUnordered { doc ->
            val writer = StringWriter()
            codec.encode(JsonWriter(writer, jws), doc, context)
            writer.toString()
        }.map {
            writeUtf8(it)
            writeUtf8("\n")
            Unit
        }
}

@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
fun BufferedSink.csvExport(
    client: MongoClient,
    database: String,
    collection: String,
    projection: Bson,
    filter: Bson,
    limit: Int? = null,
    arrayField: String?,
    dateFormatter: DateTimeFormatter,
    includeHeader: Boolean = true,
    delimiter: String = ","
): Flow<Unit> {
    val pipeline = buildList<Bson> {
        add(Aggregates.match(filter))
        limit?.also { add(limit(limit)) }
        arrayField?.also { add(Aggregates.unwind("\$$it")) }
        add(Aggregates.project(projection))
    }

    val columns = projection.toBsonDocument().filterValues { it != BsonInt32(0) }.keys

    if (includeHeader) {
        writeUtf8(columns.joinToString(delimiter))
        writeUtf8("\n")
    }

    val cws = CsvWriterSettings(delimiter, dateFormatter, columns)
    val codec = RawBsonDocumentCodec()
    val context: EncoderContext = EncoderContext.builder().build()

    return client
        .getDatabase(database)
        .getCollection(collection, RawBsonDocument::class.java)
        .aggregate(pipeline)
        .asFlow()
        .parMapUnordered { doc ->
            val writer = StringWriter()
            codec.encode(CsvWriter(writer, cws), doc, context)
            writer.toString()
        }
        .map {
            writeUtf8(it)
            writeUtf8("\n")
            Unit
        }
}