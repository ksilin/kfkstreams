package com.example

import java.time.OffsetDateTime

import com.fasterxml.jackson.databind.{ JsonNode, ObjectMapper }
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.scalatest.{ FreeSpec, MustMatchers }

case class TestClass(name: String, value: Int, dt: OffsetDateTime)

class JacksonSpec extends FreeSpec with MustMatchers {

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())

  val data = TestClass("test", 123, OffsetDateTime.now())

  "must serialize scala case class instances" in {

    val node: JsonNode = mapper.valueToTree(data)
    println(node)
    val str = node.toString
    println(str)
    val str2 = mapper.writeValueAsString(data)
//    val parsed = mapper.readValue[TestClass](node) // does not compile
    val parsed2: TestClass = mapper.readValue[TestClass](str2)
    println(parsed2)
  }

}
