package com.atguigu.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.codehaus.jackson.map.deser.std.StringDeserializer

import java.util.Properties

object MyKafkaUtil {

  //创建配置信息的对象
  private val properties: Properties = PropertiesUtil.load("config.properties")

  //2.用于初始化链接到集群的地址
  private val brokers: String = properties.getProperty("kafka.broker.list")

  //TODO
  // 创建DStream,返回接收到的输入数据
  // LocationStrategies: 根据给定的主题和集群创建consumer
  // LocationStrategies.PreferConsisent:持续的在所有Executor之间分配分区
  // ConsumerStrategies:选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe:订阅一系列主题

  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    //3.Kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "testTopic"
    )
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }


}
