package com.xuzq.sparkHbase

import java.util.Date

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Delete, Scan, ConnectionFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by xuzq on 2016/4/21.
  */
object DeleteNewsInHbase {
  def main(args: Array[String]): Unit ={
    val conf = HBaseConfiguration.create()
    val pattern = new Regex("_\\d{1,2}.[a-z]+")
    conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")
    val conn = ConnectionFactory.createConnection(conf)

    //set table name and get the table
    val userTable = TableName.valueOf("public_opinion:metadata")
    val table = conn.getTable(userTable)

    val s = new Scan()

    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("url"))
    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("timestamp"))

    val middleUrlList = new ArrayBuffer[String]()

    val rowKeyList = new ArrayBuffer[Array[Byte]]()

    val scanner = table.getScanner(s)
    var eachRow = scanner.next
    while (eachRow != null) {
      val middleUrl = Bytes.toString(eachRow.getValue(Bytes.toBytes("f2"), Bytes.toBytes("url")))
      if(!(pattern findAllIn middleUrl).isEmpty){
        middleUrlList.append(middleUrl)
        rowKeyList.append(eachRow.getRow)
      }
      eachRow = scanner.next
    }

    middleUrlList.foreach(println)

   /* rowKeyList.foreach(
      eachLine =>{
        val d = new Delete(eachLine)
        table.delete(d)
      }
    )*/
  }
}
