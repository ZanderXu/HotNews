package com.xuzq.sparkHbase

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.LinkedHashMap
import scala.io.Source
import scala.util.Random
import scala.util.matching.Regex

/**
  * Created by xuzq on 2016/4/13.
  * Get message from Hbase and write the message into Hour,Day and Week File.
  */
object GetMessageFromHbase {

  def countKeyWords(table: Table, time: String, hotNum: Int): LinkedHashMap[String, Int] = {

    val s = new Scan()
    //According to the input Time to set startRow and stopRow Begin
    val cal = Calendar.getInstance()
    time match {
      case "Hour" => cal.add(Calendar.HOUR, -1)
      case "Day" => cal.set(Calendar.HOUR_OF_DAY, 0);cal.set(Calendar.MINUTE, 0);cal.set(Calendar.SECOND, 0);
      case "Week" => cal.add(Calendar.DATE, -7)
    }

    val starRowKey = new ArrayBuffer[Byte]()
    starRowKey ++= Bytes.toBytes(-1*(new Date().getTime))
    starRowKey ++= Bytes.toBytes("00000000000000000000000000000000")

    val stopRowKey = new ArrayBuffer[Byte]()
    stopRowKey ++= Bytes.toBytes(-1*(cal.getTime.getTime))
    stopRowKey ++= Bytes.toBytes("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

    s.setStartRow(starRowKey.toArray)
    s.setStopRow(stopRowKey.toArray)
    //According to the input Time to set startRow and stopRow End

    s.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))
    val scanner = table.getScanner(s)

    //save the result of keyWords
    val keyWordsList = new ArrayBuffer[String]()

    //Get all keywords in the input Time and add them into keyWordsList
    var eachRow = scanner.next()
    while (eachRow != null) {
      keyWordsList.append(Bytes.toString(eachRow.getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))))
      eachRow = scanner.next()
    }
    scanner.close()

    //count the keyWords and save into keyWordsMap
    var keyWordsMap = LinkedHashMap[String, Int]()
    keyWordsList.foreach(
      keyWord => {
        keyWordsMap.contains(keyWord) match {
          case true => keyWordsMap += (keyWord -> (keyWordsMap.get(keyWord).get + 1))
          case false => keyWordsMap += (keyWord -> 1)
        }
      }
    )
    //sort the map by value
    val sortedMap = mutable.LinkedHashMap(keyWordsMap.toSeq.sortWith(_._2 > _._2): _*)

    if(hotNum > sortedMap.size) sortedMap else sortedMap.take(hotNum)
  }

  //According to keyWords and get the news information for each keyword from HBase
  def getHotNewsMessage(table: Table, time: String, sortedKeyWords: List[String]): Array[List[String]] = {
    val resultList = new ArrayBuffer[List[String]]()
    val regexforTitle = new StringBuilder()
    val linesList = Source.fromFile("/home/spark/xuzq/hotNews/RegexforTitle.txt").getLines.toList
    //val linesList = Source.fromFile("C:\\Xuzq\\RegexforTitle.txt").getLines.toList
    for (line <- linesList){
      if(line != ""){
        if(line != linesList(linesList.size - 1)){
          regexforTitle.append(line + "|")
        }else{
          regexforTitle.append(line)
        }
      }
    }
    val patternForTitle = new Regex(regexforTitle.toString)

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = Calendar.getInstance()
    time match {
      case "Hour" => cal.add(Calendar.HOUR, -1)
      case "Day" => cal.set(Calendar.HOUR_OF_DAY, 0);cal.set(Calendar.MINUTE, 0);cal.set(Calendar.SECOND, 0);
      case "Week" => cal.add(Calendar.DATE, -7)
    }
    val starRowKey = new ArrayBuffer[Byte]()
    starRowKey ++= Bytes.toBytes(-1 * (new Date().getTime))
    starRowKey ++= Bytes.toBytes("00000000000000000000000000000000")

    val stopRowKey = new ArrayBuffer[Byte]()
    stopRowKey ++= Bytes.toBytes(-1 * (cal.getTime.getTime))
    stopRowKey ++= Bytes.toBytes("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
    var index = 0

    for(keyword <- sortedKeyWords){

      var countNum = 0
      val s = new Scan()
      s.setStartRow(starRowKey.toArray)
      s.setStopRow(stopRowKey.toArray)

      s.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))
      s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("url"))
      s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("title"))
      s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("timestamp"))

      val scanFilter = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("keywords"), CompareOp.EQUAL, Bytes.toBytes(keyword))
      s.setFilter(scanFilter)
      val scanner = table.getScanner(s)

      val middleList = new ArrayBuffer[Result]()

      var result = scanner.next
      while(result != null){
        if(Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))) != null){
          middleList.append(result)
        }
        result = scanner.next
      }
      scanner.close()

      val selectTitle = (new Random).nextInt(middleList.size)

      var orgTitle = Bytes.toString(middleList(selectTitle).getValue(Bytes.toBytes("f2"), Bytes.toBytes("title")))
      while(!(patternForTitle findAllIn orgTitle).isEmpty){
        orgTitle = patternForTitle replaceFirstIn(orgTitle, "")
      }
      resultList.append(List((index + 1).toString, Bytes.toString(middleList(selectTitle).getValue(Bytes.toBytes("f1"), Bytes.toBytes("keywords"))),Bytes.toString(middleList(selectTitle).getValue(Bytes.toBytes("f2"), Bytes.toBytes("url"))), orgTitle))

      println(df.format(new Date()) + " keyword and count in " + time  + " " + keyword + "," + middleList.size)
    }
    resultList.toArray
  }

  //Write the Top News information into File
  def writeMessageIntoFile(time: String, resultList: Array[List[String]]): Unit = {
    val fileName = "/home/spark/xuzq/hotNews/HotNews/HotNews" + time + ".txt"
    //val fileName = "C:\\Xuzq\\HotNews" + time + ".txt"
    val writer = new PrintWriter(new File(fileName))
    for (eachLine <- resultList) {
      for (element <- eachLine) {
        writer.write(element.toString + "&&&&&")
      }
      writer.write("\n")
    }
    writer.close()
  }

  def getHotNewsByTime(table: Table, time: String, top: Int): Unit = {
    val hotNewsMessage = getHotNewsMessage(table, time, countKeyWords(table, time, top).keySet.toList)
    writeMessageIntoFile(time, hotNewsMessage)
  }

  def main(args: Array[String]): Unit = {
    //New HbaseConfiguration and init
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")
    val conn = ConnectionFactory.createConnection(conf)

    //set table name and get the table
    val userTable = TableName.valueOf("public_opinion:metadata")
    val table = conn.getTable(userTable)

    getHotNewsByTime(table, "Hour", 20)
    getHotNewsByTime(table, "Day", 20)
    getHotNewsByTime(table, "Week", 20)
  }
}
