package com.mongodb.fastexport

import com.github.ajalt.clikt.testing.test
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CliTests {

    private val uri = "mongodb://localhost:27017"

    @Test
    fun testIncludeFilter() {
        val command = Cli()
        val result = command.test("--uri $uri --nsInclude test.taxlots ")
        println(result.output)
        assertTrue {
            with(result.output) {
                contains("test.taxlots")
                !contains("test.people")
            }
        }
        assertEquals(0, result.statusCode)
    }

    @Test
    fun testExcludeFilter() {
        val command = Cli()
        val result = command.test("--uri $uri --nsExclude test.taxlots ")
        println(result.output)
        assertTrue {
            with(result.output) {
                !contains("test.taxlots")
                contains("test.people")
            }
        }
        assertEquals(0, result.statusCode)
    }
}