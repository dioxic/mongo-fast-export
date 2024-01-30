package com.mongodb.fastexport

import com.github.ajalt.clikt.testing.test
import com.mongodb.ConnectionString
import org.junit.jupiter.api.assertThrows
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CliTests {

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
    fun testMandatoryFieldArgsForCsv() {
        val command = Cli()
        val ex = assertThrows<IllegalArgumentException> {
            command.test("--uri $uri -d test -c taxlots --csv")
        }
    }

    @Test
    fun testCollectionExists() {
        val command = Cli()
        val result = command.test("--uri $uri -d test -c people ")
        println(result.output)
        assertTrue {
            with(result.output) {
                contains("empty")
            }
        }
        assertEquals(0, result.statusCode)
    }

    @Test
    fun testQuery() {
        val command = Cli()
        val query1 = dateQuery("2024-01-01T12:00:00.000Z")
        val query2 = dateQuery("2025-01-01T12:00:00.000Z")
        assertTrue {
            with(command.test("--uri $uri -d test -c taxlots -q '$query1'")) {
//                println("query1 output: $output")
                output.contains("\$oid")
                statusCode == 0
            }
        }
        assertTrue {
            with(command.test("--uri $uri -d test -c taxlots -q '$query2'")) {
//                println("query2 output: $output")
                !output.contains("\$oid")
                statusCode == 0
            }
        }
    }

    @Test
    fun testProjectionSimple() {
        val command = Cli()
        val fields = "firstName, lastName"
        with(command.test("--uri $uri -d test -c taxlots --fields '$fields'")) {
            println("output: $output")
            assertEquals("""
                {
                  "_id": {
                    "${'$'}oid": "65b78d05d12faf22a9edaeaf"
                  },
                  "firstName": "Bob"
                }
            """.trimIndent(), output)
            assertEquals(0, statusCode)
        }
    }
}