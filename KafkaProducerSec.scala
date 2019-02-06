package com.structure.test
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.util.Random
class KafkaProducerSec {
  def kafkaProd(logMessage: String): Unit = {
    try {
      val topic = "sampletopic"
      val brokers = "<Kafka CLuster details with port number>"
      val rnd = new Random()
      val producerprops = new Properties()
      producerprops.put("bootstrap.servers", brokers)
      producerprops.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producerprops.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](producerprops)
      val data = new ProducerRecord[String, String](topic, logMessage)
      println(data)
      producer.send(data)
      producer.close()
    }
      }
}
