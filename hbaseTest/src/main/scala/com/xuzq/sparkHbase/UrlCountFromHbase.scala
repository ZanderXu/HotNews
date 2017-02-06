package com.xuzq.sparkHbase

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuzq on 2016/4/20.
  */
object UrlCountFromHbase {
  def main(args: Array[String]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val dfHMS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val newsWebsiteList = List("163","sina","cyol","china","xinhuanet","cankaoxiaoxi","sohu","people","chinanews","gmw","wenweipo","cri","cnr","stnn","qq","caixin","cctv","toutiao")

    val timeTest = "Week"

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")
    val conn = ConnectionFactory.createConnection(conf)

    //set table name and get the table
    val userTable = TableName.valueOf("public_opinion:metadata")
    val table = conn.getTable(userTable)

    val s = new Scan()

    val cal = Calendar.getInstance()

    timeTest match {
      case "Hour" => cal.add(Calendar.HOUR, -1)
      case "Day" => cal.add(Calendar.DATE, -1)
      case "Week" => cal.add(Calendar.DATE, -7)
    }

    val starRowKey = new ArrayBuffer[Byte]()
    starRowKey ++= Bytes.toBytes(-1 * (new Date().getTime))
    starRowKey ++= Bytes.toBytes("00000000000000000000000000000000")

    val stopRowKey = new ArrayBuffer[Byte]()
    stopRowKey ++= Bytes.toBytes(-1 * (cal.getTime.getTime))
    stopRowKey ++= Bytes.toBytes("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

    s.setStartRow(starRowKey.toArray)
    s.setStopRow(stopRowKey.toArray)

    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("url"))
    s.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("timestamp"))


    val middleUrlList = new ArrayBuffer[String]()
    var middleTimeMap = Map[String, String]()

    val scanner = table.getScanner(s)
    var eachRow = scanner.next
    while (eachRow != null) {
      val middleUrl = Bytes.toString(eachRow.getValue(Bytes.toBytes("f2"), Bytes.toBytes("url")))
      val middleTime = Bytes.toString(eachRow.getValue(Bytes.toBytes("f2"), Bytes.toBytes("timestamp")))
      middleUrlList.append(middleUrl.split("/")(0) + "//" + middleUrl.split("/")(2))
      val currentTime = df.format(new Date().getTime)
      val weekBeforeTime = df.format(cal.getTime.getTime)
      if(middleTime < weekBeforeTime) middleTimeMap += (middleUrl -> middleTime)
      eachRow = scanner.next
    }

    var middleWebsiteMap = Map[String, Int]()
    middleUrlList.foreach(
      keyWord => {
        middleWebsiteMap.contains(keyWord) match {
          case true => middleWebsiteMap += (keyWord -> (middleWebsiteMap.get(keyWord).get + 1))
          case false => middleWebsiteMap += (keyWord -> 1)
        }
      }
    )
  mutable.LinkedHashMap(middleWebsiteMap.toSeq.sortWith(_._2 > _._2): _*).foreach(println)

    var resultWebsiteMap = mutable.LinkedHashMap[String, Int]()

    newsWebsiteList.foreach(
      webSite => {
        middleWebsiteMap.foreach(
          eachNewsWebsite  => {
            if(eachNewsWebsite._1.indexOf(webSite) != -1){
              if(resultWebsiteMap.contains(webSite)){
                resultWebsiteMap += (webSite -> (resultWebsiteMap.get(webSite).get + eachNewsWebsite._2))
              }else{
                resultWebsiteMap += (webSite -> eachNewsWebsite._2)
              }
            }
          }
        )
      }
    )

    //sort the map by value
    val sortedResult = mutable.LinkedHashMap(resultWebsiteMap.toSeq.sortWith(_._2 > _._2): _*)

    println(dfHMS.format(new Date().getTime) + " The Result of Url Count is:")

    sortedResult.foreach(println)

    if(middleTimeMap.size > 0){
      println(dfHMS.format(new Date().getTime) + " The result of the message time old is:")
      middleTimeMap.foreach(eachMap => println(eachMap._1 + "  " + eachMap._2))
    }
  }
}
