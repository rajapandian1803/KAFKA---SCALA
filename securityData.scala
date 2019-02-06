package com.structure.test
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object securityData {

  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("dd-MM-yyyy")
    val kafProd = new KafkaProducerSec()
    kafProd.kafkaProd("Security_Normalised table created on " + format.format(Calendar.getInstance().getTime()))
    println("Kafka Done")

    val kafCons = new KafkaConsumeSec
    kafCons.KafkaConsu
    println("Kafka Consumer Done")
  }
}
