package com.xuzq.sparkHbase

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Delete, Scan, ConnectionFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by xuzq on 2016/4/22.
  */
object DeleteNewsInHbaseForUrl {
  def main(args: Array[String]): Unit ={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "mhdp1, mhdp2, hdp1, hdp2, hdp3")
    val conn = ConnectionFactory.createConnection(conf)

    //set table name and get the table
    val userTable = TableName.valueOf("public_opinion:metadata")
    val table = conn.getTable(userTable)

    val url = "http://epaper.xxsb.com:80/showNews/2016-04-22/304943.html"

    val s = new Scan()

    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("url"))
    s.setFilter(new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("url"), CompareOp.EQUAL, Bytes.toBytes(url)))

    val middleUrlList = ArrayBuffer[Array[Byte]]()

    val scanner = table.getScanner(s)
    var eachRow = scanner.next
    while (eachRow != null) {
      middleUrlList.append(eachRow.getRow)
      eachRow = scanner.next
    }

    middleUrlList.foreach(println)


    for(deleteObj <- middleUrlList if deleteObj != middleUrlList.apply(0)){
      val d = new Delete(deleteObj)
      table.delete(d)
    }
  }

}
