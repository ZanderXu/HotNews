package com.xuzq

import java.io.{File, PrintWriter}
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.{HTableDescriptor,HColumnDescriptor,HBaseConfiguration,TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory,Put,Get,Delete,Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {

    //println((0 to 2 - 1))
    /*val test1 = List(1, "1http://news.cctv.com/2016/04/06/ARTIz6lJysznk5m15sB9YrGV160406.shtml", "test1")
    val test2 = List(2, "2http://news.cctv.com/2016/04/06/ARTIz6lJysznk5m15sB9YrGV160406.shtml", "test2")

    val test = Array(test1,test2)

    val writer = new PrintWriter(new File("C:\\Xuzq\\scalaWriteFile.txt"))

    for(eachLine <- test){
      for(element <- eachLine){
        writer.write(element.toString + "&&&&&")
      }
      writer.write("\n")
    }

    writer.write("Hello Scala")

    writer.close()

    val lines = Source.fromFile("C:\\Xuzq\\scalaWriteFile.txt").getLines()
    lines.foreach(
      line => {
        val list = line.split("&&&&&")
        list.foreach(print)
        println
      }
    )*/

    /*println(-new Date().getTime + "00000000000000000000000000000000")
    var cal  = Calendar.getInstance()
    cal.add(Calendar.HOUR,-1)
    println(cal.getTime())
    println(-cal.getTime().getTime + "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

    var cal1:Calendar = Calendar.getInstance()
    cal1.add(Calendar.DATE,-1)
    println(cal1.getTime())
    println(-cal1.getTime().getTime + "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")*/

    val conf = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")

    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)

    //本例将操作的表名
    val userTable = TableName.valueOf("public_opinion:metadata")

    val table = conn.getTable(userTable)

    val dateTime = new Date().getTime
    val rowKey = new ArrayBuffer[Byte]()

    rowKey ++= Bytes.toBytes(-dateTime)
    rowKey ++= Bytes.toBytes("729997E78FF3DDA6CAA342A2C9F00BBB")


    val p = new Put(rowKey.toArray)
    p.addColumn("f1".getBytes(), "content".getBytes(), "基于java语言开发的轻量级的中文分词工具包1".getBytes())
    p.addColumn("f2".getBytes(), "url".getBytes(), "http://testHour.com1".getBytes())
    p.addColumn("f2".getBytes(), "timestamp".getBytes(), "1459928169591".getBytes())
    p.addColumn("f2".getBytes(), "title".getBytes(), "houtTestdsfdsafadsfdsfsdfds".getBytes())
    p.addColumn("f1".getBytes(), "keywords".getBytes(), "java轻量级4444444".getBytes())

    table.put(p)

    val row = new Get(rowKey.toArray)
    val HBaseRow = table.get(row)
    var result: String = null
    if(HBaseRow != null && !HBaseRow.isEmpty){
      result = Bytes.toString(HBaseRow.getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords")))
    }
    else{
      result = null
    }

    println(result)

    /*val s = new Scan()
    s.addColumn("f1".getBytes,"keywords".getBytes)
    val scanner = table.getScanner(s)
    val keyWordsList = new ArrayBuffer[String]()

    var eachRow = scanner.next()
    while(eachRow != null){
      keyWordsList.append(Bytes.toString(eachRow.getValue("f1".getBytes,"keywords".getBytes)))
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
    scanner.close()*/
  }

}
