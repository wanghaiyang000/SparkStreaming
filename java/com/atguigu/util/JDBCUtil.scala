package com.atguigu.util


import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object JDBCUtil {

  //初始化连接池
  val dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {


    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")

    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))

    DruidDataSourceFactory.createDataSource(properties)
  }

  /**
   * 返回一个数据库连接
   *
   * @return
   */
  def getConnection: Connection = {
    dataSource.getConnection
  }

  def getBlackList(connection: Connection): List[Int] = {
    val preparedStatement: PreparedStatement = connection.prepareStatement("select userid from spark2020.black_list")
    val resultSet: ResultSet = preparedStatement.executeQuery()
    val listBuffer: ListBuffer[Int] = mutable.ListBuffer.empty[Int]
    while (resultSet.next) {
      listBuffer += resultSet.getInt(1)
    }

    listBuffer.toList
  }


}
