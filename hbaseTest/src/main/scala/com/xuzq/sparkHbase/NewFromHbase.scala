package com.xuzq.sparkHbase

import java.util.{Date, Calendar}

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuzq on 2016/4/19.
  */
object NewFromHbase {

  def main(args: Array[String]): Unit ={
    //val url = "http://art.china.com/news/hwdt/11159338/20160420/22477871_6.html"
    val url = "http://epaper.xxsb.com:80/showNews/2016-04-22/304943.html"
    val keyword = "翻天"
    val titleTest = "还有“女流氓式”？日本厂商推出水手服居家服"

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")
    val conn = ConnectionFactory.createConnection(conf)

    //set table name and get the table
    val userTable = TableName.valueOf("public_opinion:metadata")
    val table = conn.getTable(userTable)

    val s = new Scan()

    s.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))
    s.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("content"))
    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("url"))
    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("title"))
    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("timestamp"))

    val middleList = new ArrayBuffer[List[String]]()
    val scanFilter = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("url"), CompareOp.EQUAL, Bytes.toBytes(url))
    //val scanFilter = new SingleColumnValueFilter(Bytes.toBytes("f2"), Bytes.toBytes("title"), CompareOp.EQUAL, Bytes.toBytes(titleTest))
    //val scanFilter = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("keywords"), CompareOp.EQUAL, Bytes.toBytes(keyword))
    s.setFilter(scanFilter)
    val scanner = table.getScanner(s)
    var eachRow = scanner.next
    while (eachRow != null) {
      if(Bytes.toString(eachRow.getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))) != null){
        val middle = List(Bytes.toString(eachRow.getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))), Bytes.toString(eachRow.getValue(Bytes.toBytes("f1"), Bytes.toBytes("content"))), Bytes.toString(eachRow.getValue(Bytes.toBytes("f2"), Bytes.toBytes("url"))), Bytes.toString(eachRow.getValue(Bytes.toBytes("f2"), Bytes.toBytes("title"))), Bytes.toString(eachRow.getValue(Bytes.toBytes("f2"), Bytes.toBytes("timestamp"))))
        middleList.append(middle)
      }
      eachRow = scanner.next
    }

    middleList.toArray.foreach(
      message =>{
        println("keyWord: " + message(0) + " content: " + message(1) + " url: " + message(2) + " title: " + message(3) + " time: " + message(4))
      }
    )
  }
}
