package com.xuzq.sparkHbase

import java.util.{Date, Calendar}

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.{HTableDescriptor, HColumnDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Get, Delete, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
  * @author ${user.name}
  */
object SparkHbase {

  def main(args: Array[String]) {

    val conf = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")

    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)

    //本例将操作的表名
    val userTable = TableName.valueOf("public_opinion:metadata")

    val table = conn.getTable(userTable)

    /*val p = new Put("-1459928169599729997E78FF3DDA6CAA342A2C9F00ACF".getBytes())
    p.addColumn("f1".getBytes(), "content".getBytes(), "基于java语言开发的轻量级的中文分词工具包".getBytes())
    p.addColumn("f2".getBytes(), "url".getBytes(), "http://niwota1.com".getBytes())
    p.addColumn("f2".getBytes(), "timestamp".getBytes(), "145992816959".getBytes())
    p.addColumn("f2".getBytes(), "title".getBytes(), "java轻量级".getBytes())

    table.put(p)*/
    /*val p = new Put("-1459928169599729997E78FF3DDA6CAA342A2C9F00ACF".getBytes())
    p.addColumn("f1".getBytes(), "keywords".getBytes(), "java轻量级".getBytes())

    table.put(p)*/

    /* val row = new Get("-1459928169599729997E78FF3DDA6CAA342A2C9F00ACF".getBytes())
     val HBaseRow = table.get(row)
     var result: String = null
     if(HBaseRow != null && !HBaseRow.isEmpty){
       result = Bytes.toString(HBaseRow.getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords")))
     }
     else{
       result = null
     }

     println(result)*/

    val s = new Scan()
    val scanner = table.getScanner(s)
    val keyWordsList = new ArrayBuffer[String]()

    var eachRow = scanner.next()
    while(eachRow != null){
      keyWordsList.append(Bytes.toString(eachRow.getValue("f1".getBytes,"url".getBytes)))
      eachRow = scanner.next()
    }

    //var keyWordsMap = new mutable.HashMap[String, String]()

    var keyWordsMap = Map[String, Int]()
    for(keyWord <- keyWordsList){
      if(!keyWordsMap.contains(keyWord)){
        keyWordsMap += (keyWord -> 1)
      }
      else{
        val keyWordCount: Int = keyWordsMap.get(keyWord).get
        val keyWordCountNew = keyWordCount + 1
        keyWordsMap += (keyWord -> keyWordCountNew)
      }
    }

    keyWordsList.foreach(println)

    keyWordsMap.foreach(println)
    scanner.close()


    /*val s = new Scan()
    val filterList = new FilterList()

    val scanFilter = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("keywords"), CompareOp.EQUAL, Bytes.toBytes("java轻量级1"))

    filterList.addFilter(scanFilter)
    s.setFilter(filterList)

    s.setMaxResultSize(1)

    val scanner = table.getScanner(s)

    val resultList = new ArrayBuffer[List[String]]()

    val eachRow = scanner.next()

    if(eachRow != null){
      val middle = List(Bytes.toString(eachRow.getValue("f2".getBytes,"url".getBytes)), Bytes.toString(eachRow.getValue("f2".getBytes,"title".getBytes)))
      resultList.append(middle)
    }

    resultList.foreach(
      message =>{
        message.foreach(println)
      })
    scanner.close()*/
    /*val s = new Scan()
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -1)

    val starRowKey = new ArrayBuffer[Byte]()
    starRowKey ++= Bytes.toBytes(-new Date().getTime)
    starRowKey ++= Bytes.toBytes("00000000000000000000000000000000")

    val stopRowKey = new ArrayBuffer[Byte]()
    stopRowKey ++= Bytes.toBytes(-cal.getTime.getTime)
    stopRowKey ++= Bytes.toBytes("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

    s.setStartRow(starRowKey.toArray)
    s.setStopRow(stopRowKey.toArray)

    s.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))
    val scanner = table.getScanner(s)

    //save the result of keyWords
    val keyWordsList = new ArrayBuffer[String]()

    var eachRow = scanner.next()
    while (eachRow != null) {
      keyWordsList.append(Bytes.toString(eachRow.getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))))
      eachRow = scanner.next()
    }

    //count the keyWords and save into keyWordsMap
    var keyWordsMap = Map[String, Int]()

    for (keyWord <- keyWordsList) {
      if (!keyWordsMap.contains(keyWord)) {
        keyWordsMap += (keyWord -> 1)
      }
      else {
        val keyWordCount: Int = keyWordsMap.get(keyWord).get
        val keyWordCountNew = keyWordCount + 1
        keyWordsMap += (keyWord -> keyWordCountNew)
      }
    }

    keyWordsMap.foreach(
      mapTest =>{
        println(mapTest._1)
        println(mapTest._2)
      }
    )
    scanner.close()
  }*/
  }

}
