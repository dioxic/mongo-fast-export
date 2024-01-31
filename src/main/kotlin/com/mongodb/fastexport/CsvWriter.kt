package com.mongodb.fastexport

import org.bson.*
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import java.io.Writer
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*

class CsvWriter(
    private val writer: Writer,
    private val csvSettings: CsvWriterSettings
) : AbstractBsonWriter(csvSettings) {

    private val map = HashMap<String, String>(csvSettings.columns.size)
    private val fieldStack = Stack<String>()
    private val runningCoordinates: String = ""

    init {
        context = Context(null, BsonContextType.TOP_LEVEL)
    }

    private val coordinates
        get() = fieldStack.joinToString(".")

    override fun flush() {
        writer.flush()
    }

    override fun doWriteName(name: String) {
        fieldStack[fieldStack.lastIndex] = name
    }

    override fun doWriteStartDocument() {
        val contextType = when (state) {
            State.SCOPE_DOCUMENT -> BsonContextType.SCOPE_DOCUMENT
            else -> BsonContextType.DOCUMENT
        }
        context = Context(context as Context, contextType)
        fieldStack.push("PLACEHOLDER")
    }

    override fun doWriteEndDocument() {
        when (context.contextType) {
            BsonContextType.SCOPE_DOCUMENT -> {
                context = context.parentContext
                writeEndDocument()
            }

            else -> context = context.parentContext
        }

        if (context.contextType == BsonContextType.TOP_LEVEL) {
            writer.write(csvSettings.columns
                .mapNotNull { map[it] }
                .joinToString(csvSettings.delimiter))
        } else {
            fieldStack.pop()
        }
    }

    override fun doWriteStartArray() {
        context = Context(context as Context, BsonContextType.ARRAY)
    }

    override fun doWriteEndArray() {
        context = context.parentContext
    }

    override fun doWriteBinaryData(value: BsonBinary) {
        error("binary bson type not supported")
    }

    override fun doWriteBoolean(value: Boolean) {
        map[coordinates] = value.toString()
//        writer.write(value.toString())
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteDateTime(value: Long) {
        map[coordinates] =
            csvSettings.dateFormatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC))
//        writer.write(value.toString())
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteDBPointer(value: BsonDbPointer) {
        error("dbpointer bson type not supported")
    }

    override fun doWriteDouble(value: Double) {
        map[coordinates] = value.toString()
//        writer.write(value.toString())
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteInt32(value: Int) {
        map[coordinates] = value.toString()
//        writer.write(value.toString())
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteInt64(value: Long) {
        map[coordinates] = value.toString()
//        writer.write(value.toString())
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteDecimal128(value: Decimal128) {
        map[coordinates] = value.toString()
//        writer.write(value.toString())
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteJavaScript(value: String) {
        error("javascript bson type not supported")
    }

    override fun doWriteJavaScriptWithScope(value: String) {
        error("javascript bson type not supported")
    }

    override fun doWriteMaxKey() {
        TODO("Not yet implemented")
    }

    override fun doWriteMinKey() {
        TODO("Not yet implemented")
    }

    override fun doWriteNull() {
        map[coordinates] = "null"
//        writer.write("null")
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteObjectId(value: ObjectId) {
        map[coordinates] = value.toHexString()
//        writer.write(value.toHexString())
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteRegularExpression(value: BsonRegularExpression) {
        error("regular expression bson type not supported")
    }

    override fun doWriteString(value: String) {
        map[coordinates] = value
//        writer.write(value)
//        writer.write(csvSettings.delimiter)
    }

    override fun doWriteSymbol(value: String) {
        error("symbol bson type not supported")
    }

    override fun doWriteTimestamp(value: BsonTimestamp) {
        error("bsontimestamp type not supported")
    }

    override fun doWriteUndefined() {
        error("undefined bson type not supported")
    }

    inner class Context(parentContext: Context?, contextType: BsonContextType?) :
        AbstractBsonWriter.Context(parentContext, contextType) {
        override fun getParentContext(): Context {
            return super.getParentContext() as Context
        }
    }
}

