package com.mongodb.fastexport

import com.mongodb.ConnectionString
import kotlinx.coroutines.runBlocking
import okio.Path.Companion.toPath
import okio.buffer
import okio.fakefilesystem.FakeFileSystem
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class EngineTests {

    private val uri = "mongodb://localhost:27017"
    private val client = ConnectionString(uri).createClient()

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
            jsonExport(
                client = client,
                database = "test",
                collection = "taxlots",
                fields = listOf("firstName", "taxlots.rec"),
                filter = dateQuery("2024-01-01T12:00:00.000Z").toQueryBson(),
                sink = fs.sink(fakeFile).buffer()
            )
        }
        val expected = """
            {"_id": "myId", "firstName": "Bob", "taxlots": [{"rec": 0}, {"rec": 1}, {"rec": 2}]}
            
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
            csvExport(
                client = client,
                database = "test",
                collection = "taxlots",
                fields = listOf("taxlots.rec", "firstName"),
                filter = dateQuery("2024-01-01T12:00:00.000Z").toQueryBson(),
                arrayField = "taxlots",
                sink = fs.sink(fakeFile).buffer(),
            )
        }
        val expected = """
            0,Bob
            1,Bob
            2,Bob
            
        """.trimIndent()
        assertEquals(expected, fs.read(fakeFile) { readUtf8() })
    }

}