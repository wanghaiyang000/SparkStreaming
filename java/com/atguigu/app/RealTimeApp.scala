package com.atguigu.app

import com.atguigu.util.{JDBCUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Connection

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    //TODO 获取configurationProperties
    val config = PropertiesUtil.load("config.properties")
    //TODO 1.生成kafaDstream
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeAPP")
    val ssc = new StreamingContext(conf, Seconds(3))
    val kafkaSource: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        List(config.getProperty("kafka.topic")),
        Map(
          "bootstrap.servers" -> config.getProperty("kafka.broker.list"),
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "RT01",
          "auto.offset.reset" -> "earliest"
        )
      )
    )
    val odsLog: DStream[UserAction] = kafkaSource.map(_.value().split(" ")).map(
      fields => {
        UserAction(
          fields(0).toLong,
          fields(1),
          fields(2),
          fields(3).toInt,
          fields(4).toInt
        )
      }
    )
    odsLog.print()

    //TODO 2.过滤黑名单数据
    val connection: Connection = JDBCUtil.getConnection
    val blackList: List[Int] = JDBCUtil.getBlackList(connection)
    val fileteredOdsLog: DStream[UserAction] = odsLog.filter(x => {
      connection.close()
      !blackList.contains(x.userId)

    }
      )


    //TODO 3.统计用户点击广告数量

    //TODO 4.用结果更新点击表格，并根据表格数据内容更新黑名单

    ssc.start()
    ssc.awaitTermination()

  }

}

case class UserAction(timestamp: Long, area: String, city: String, userId: Int, adId: Int)

case class UserClick(dt: String, userId: Int, adID: Int, count: Int)