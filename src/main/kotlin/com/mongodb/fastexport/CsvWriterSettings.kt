package com.mongodb.fastexport

import org.bson.BsonWriterSettings
import java.time.format.DateTimeFormatter

class CsvWriterSettings(
    val delimiter: String,
    val dateFormatter: DateTimeFormatter,
    val columns: Collection<String>,
    val unwindField: String?,
) : BsonWriterSettings()