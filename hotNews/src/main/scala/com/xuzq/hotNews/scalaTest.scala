package com.xuzq.hotNews

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import com.xuzq.SplitWord.SplitWord
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTable, ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by xuzq on 2016/4/13.
  */
object scalaTest {

  def main(args: Array[String]): Unit = {

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //val sparkConf = new SparkConf().setAppName("HotNews").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setMaster("local").setAppName("HotNews").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")

    conf.set(TableInputFormat.INPUT_TABLE, "public_opinion:metadata")

    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("content"))
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val starRowKey = new ArrayBuffer[Byte]()
    starRowKey ++= Bytes.toBytes(-new Date().getTime)
    starRowKey ++= Bytes.toBytes("00000000000000000000000000000000")

    val stopRowKey = new ArrayBuffer[Byte]()
    stopRowKey ++= Bytes.toBytes(-cal.getTime.getTime)
    stopRowKey ++= Bytes.toBytes("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

    scan.setStartRow(starRowKey.toArray)
    scan.setStopRow(stopRowKey.toArray)

    conf.set(TableInputFormat.SCAN, Base64.encodeBytes((ProtobufUtil.toScan(scan)).toByteArray))


    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    println(df.format(new Date()) + " hBaseRDD = " + hBaseRDD.count)

    //output the data format in each line is :Map(ImmutableBytesWritable, Result)
    val contentRDD = hBaseRDD.map(_._2).map(result => (result.getRow, Bytes.toString(result.getValue("f1".getBytes, "content".getBytes))))


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val splitCountRDD = contentRDD.map(
      message => {
        val regex = new Regex("""([^0-9]*)""")
        val regexeng = new Regex("""([^a-z]+)""")
        (message._1, (new SplitWord().splitWordMethod(message._2).split(" ").map(_.toLowerCase)
          .filter(token => regex.pattern.matcher(token).matches)
          .filter(token => regexeng.pattern.matcher(token).matches)
          .filter(token => token.size >= 2).toList))
      }
    )

    println(df.format(new Date()) + " The count of the size is 0 after content splited is: " + splitCountRDD.filter(_._2.size == 0).count)

    val wordsDataFrame = splitCountRDD.filter(_._2.size > 0).map(record => Record(record._1, record._2, null)).toDF()
    wordsDataFrame.registerTempTable("words")

    val resultMap = new HotWordsExtractionNew().HotWordsExtractionModel(wordsDataFrame, Math.pow(2, 20).toInt, 20)

    println(df.format(new Date()) + " The count of result after algorithm calculated is: " + resultMap.size)


    val mapRDD  = sc.parallelize(resultMap.toSeq)

    val tableName = "public_opinion:metadata"

    mapRDD.foreachPartition(
      eachResultRDD =>{
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.defaults.for.version.skip", "true")
        conf.set(TableInputFormat.INPUT_TABLE, "public_opinion:metadata")

        val myTable = new HTable(conf, TableName.valueOf(tableName))

        myTable.setAutoFlush(false, false)

        myTable.setWriteBufferSize(3*1024*1024)

        eachResultRDD.foreach(
          result => {
            val p = new Put(result._1)
            p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("keywords"), Bytes.toBytes(result._2))
            myTable.put(p)
          }
        )
        myTable.flushCommits()
      }
    )


    /*//Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)

    //本例将操作的表名
    val userTable = TableName.valueOf("public_opinion:metadata")
    val table = conn.getTable(userTable)

    resultMap.foreach(
      result => {
        val p = new Put(result._1)
        p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("keywords"), Bytes.toBytes(result._2))
        table.put(p)
      }
    )*/

    val reduceByKeyRDD = sc.parallelize(resultMap.values.toList).map((_, 1)).reduceByKey(_+_).collect().sortBy(-_._2)

    for(i <- 0 to 19){
      println(df.format(new Date()) + " keyWord " + (i + 1) + " is: " + reduceByKeyRDD(i))
    }
    sc.stop()
  }

  case class Record(label: Array[Byte], content: List[String], keyWord: String)
}
