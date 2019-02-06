package com.structure.test
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import scala.collection.JavaConverters._
import java.util.{Collections, Properties}

class KafkaConsumeSec {
  def KafkaConsu(): Unit = {
    val TOPIC = "sampletopic"
    val brokers = "<Kafka CLuster details with port number>"
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", brokers)
    consumerProps.put("group.id", "test")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProps)
    println(TOPIC)
    consumer.subscribe(Collections.singletonList(TOPIC))
    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        println("topic" + record.topic)
        println("value" + record.value)
      }
    }
    consumer.close()
  }
}


