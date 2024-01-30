package com.mongodb.fastexport

import arrow.fx.coroutines.parMapUnordered
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Aggregates.limit
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import okio.BufferedSink
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
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.time.Duration.Companion.seconds

private val jws: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()
private val codec = RawBsonDocumentCodec()
private val context: EncoderContext = EncoderContext.builder().build()
private val engineCodecRegistry = fromRegistries(
    fromProviders(DocumentCodecProvider(BsonTypeClassMap(mapOf(BsonType.DATE_TIME to LocalDateTime::class.java)))),
    fromProviders(Jsr310CodecProvider()),
    MongoClientSettings.getDefaultCodecRegistry()
)

@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
suspend fun jsonExport(
    client: MongoClient,
    database: String,
    collection: String,
    fields: List<String>?,
    filter: Bson,
    limit: Int?,
    sink: BufferedSink,
): Int =
    sink.use { bs ->
        client
            .getDatabase(database)
            .getCollection(collection, RawBsonDocument::class.java)
            .find(filter)
            .let { cursor ->
                limit?.let { cursor.limit(limit) } ?: cursor
            }
            .projection(fields.toProjection())
            .asFlow()
            .parMapUnordered { doc ->
                val writer = StringWriter()
                codec.encode(JsonWriter(writer, jws), doc, context)
                bs.writeUtf8(writer.toString())
                bs.writeUtf8("\n")
            }.count()
    }

@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
suspend fun csvExport(
    client: MongoClient,
    database: String,
    collection: String,
    fields: List<String>,
    filter: Bson,
    limit: Int?,
    sink: BufferedSink,
    arrayField: String?,
    dateFormatter: DateTimeFormatter,
) {
    val pipeline = buildList<Bson> {
        add(Aggregates.match(filter))
        limit?.also { add(limit(limit)) }
        arrayField?.also { add(Aggregates.unwind("\$$it")) }
        add(Aggregates.project(fields.toProjection()!!))
    }

    sink.use { bs ->
        client
            .getDatabase(database)
            .getCollection(collection, Document::class.java)
            .withCodecRegistry(engineCodecRegistry)
            .aggregate(pipeline)
            .asFlow()
            .parMapUnordered { doc ->
                val flatDoc = doc.flatten(leafOnly = true)
                fields.map { field ->
                    flatDoc[field].let {
                        when (it) {
                            is LocalDateTime -> dateFormatter.format(it)
                            else -> it
                        }
                    }
                }.joinToString(",")
            }.onEach {
                bs.writeUtf8(it)
                bs.writeUtf8("\n")
            }.runningFold(0L) { accumulator, _ ->
                accumulator + 1L
            }.conflate()
            .onEach { delay(1.seconds) }
            .collect {
                println("Written $it")
            }
    }
}