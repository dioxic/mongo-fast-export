package com.mongodb.fastexport

import arrow.fx.coroutines.parMapUnordered
import com.mongodb.MongoClientSettings
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
import org.bson.BsonType
import org.bson.Document
import org.bson.RawBsonDocument
import org.bson.codecs.BsonTypeClassMap
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.EncoderContext
import org.bson.codecs.RawBsonDocumentCodec
import org.bson.codecs.configuration.CodecRegistries.fromProviders
import org.bson.codecs.configuration.CodecRegistries.fromRegistries
import org.bson.codecs.jsr310.Jsr310CodecProvider
import org.bson.conversions.Bson
import org.bson.json.JsonMode
import org.bson.json.JsonWriter
import org.bson.json.JsonWriterSettings
import java.io.StringWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private val jws: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()
private val codec = RawBsonDocumentCodec()
private val context: EncoderContext = EncoderContext.builder().build()
private val engineCodecRegistry = fromRegistries(
    fromProviders(DocumentCodecProvider(BsonTypeClassMap(mapOf(BsonType.DATE_TIME to LocalDateTime::class.java)))),
    fromProviders(Jsr310CodecProvider()),
    MongoClientSettings.getDefaultCodecRegistry()
)

@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
fun BufferedSink.jsonExport(
    client: MongoClient,
    database: String,
    collection: String,
    projection: Bson?,
    filter: Bson,
    limit: Int?,
): Flow<Unit> =
    client
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

@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
fun BufferedSink.csvExport(
    client: MongoClient,
    database: String,
    collection: String,
    projection: Bson,
    filter: Bson,
    limit: Int?,
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

    return client
        .getDatabase(database)
        .getCollection(collection, Document::class.java)
        .withCodecRegistry(engineCodecRegistry)
        .aggregate(pipeline)
        .asFlow()
        .parMapUnordered { doc ->
            val flatDoc = doc.flatten(leafOnly = true)
            columns.map { field ->
                flatDoc[field].let {
                    when (it) {
                        is LocalDateTime -> dateFormatter.format(it)
                        else -> it
                    }
                }
            }.joinToString(delimiter)
        }.map {
            writeUtf8(it)
            writeUtf8("\n")
            Unit
        }
}