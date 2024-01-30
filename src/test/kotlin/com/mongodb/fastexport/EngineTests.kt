package com.mongodb.fastexport

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import okio.Path.Companion.toPath
import okio.buffer
import okio.fakefilesystem.FakeFileSystem
import java.time.format.DateTimeFormatter
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class EngineTests {

    private val uri = "mongodb://localhost:27017"
    private val client = createClient(uri)

    @BeforeTest
    fun setup() {
        client
            .getDatabase("test")
            .getCollection("taxlots").also { coll ->
                coll.drop()
                coll.insertOne(createTaxlot())
            }
    }

    @Test
    fun engineJsonTest() {
        val fs = FakeFileSystem()
        val dir = "/dump".toPath()
        val fakeFile = dir / "fakefile"
        fs.createDirectories(dir)
        runBlocking {
            fs.sink(fakeFile).buffer().use { bs ->
                bs.jsonExport(
                    client = client,
                    database = "test",
                    collection = "taxlots",
                    projection = listOf("firstName", "taxlots.rec").toProjection(),
                    filter = dateQuery("2024-01-01T12:00:00.000Z").toQueryBson(),
                ).collect()
            }
        }

        val expected = """
            {"firstName": "Bob", "taxlots": [{"rec": 0}, {"rec": 1}, {"rec": 2}]}
            
        """.trimIndent()
        assertEquals(expected, fs.read(fakeFile) { readUtf8() })
    }

    @Test
    fun engineCsvTest() {
        val fs = FakeFileSystem()
        val dir = "/dump".toPath()
        val fakeFile = dir / "fakefile"
        fs.createDirectories(dir)
        runBlocking {
            fs.sink(fakeFile).buffer().use { bs ->
                bs.csvExport(
                    client = client,
                    database = "test",
                    collection = "taxlots",
                    projection = listOf("taxlots.rec", "firstName").toProjection()!!,
                    filter = dateQuery("2024-01-01T12:00:00.000Z").toQueryBson(),
                    arrayField = "taxlots",
                    dateFormatter = DateTimeFormatter.ISO_DATE_TIME
                ).collect()
            }
        }
        val expected = """
            taxlots.rec,firstName
            0,Bob
            1,Bob
            2,Bob
            
        """.trimIndent()
        assertEquals(expected, fs.read(fakeFile) { readUtf8() })
    }

}