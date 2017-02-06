package com.xuzq.hotNews

import java.util.Date

import com.xuzq.MD5.MessageMD5
import com.xuzq.SplitWord.SplitWord
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import scala.util.parsing.json.JSON

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64

/**
  * @author ${user.name}
  */
object HotNews {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //lines.print()

    val info = lines.map(
      line => {
        val jsonObj = JSON.parseFull(line)
        val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
        val urlmd5 = new MessageMD5().messageMD5(map.get("url").get.toString)

        val rowKey = new ArrayBuffer[Byte]()
        rowKey ++= Bytes.toBytes(-new Date().getTime())
        rowKey ++= Bytes.toBytes(urlmd5)

        (rowKey.toArray, map.get("title").get.toString, map.get("bodys").get.toString, map.get("url").get.toString, map.get("time").get.toString)
      }
    )
    info.print

    //add TimeStamp&MD5(url),content,url,time,title into HBase
    info.foreachRDD(
      rdd => rdd.foreachPartition {
        partition =>
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")
          val connection = ConnectionFactory.createConnection(conf)
          val userTable = TableName.valueOf("public_opinion:metadata")
          val table = connection.getTable(userTable)

          partition.foreach {
            element => try {

              val p = new Put(element._1)
              p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("content"), Bytes.toBytes(element._3))

              p.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("url"), Bytes.toBytes(element._4))
              p.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("timestamp"), Bytes.toBytes(element._5))
              p.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("title"), Bytes.toBytes(element._2))

              table.put(p)
            } catch {
              case _: Exception => println("raw error!")
            }
          }
          table.close()
          connection.close()
      }
    )

    /*info.filter(_._4 == null).foreachRDD(
      rdd => {
        val sc = rdd.sparkContext

        val conf = HBaseConfiguration.create()

        conf.set("hbase.zookeeper.quorum", "mhdp1.b.xuzq.com, mhdp2.b.xuzq.com, hdp1.b.xuzq.com,hdp2.b.xuzq.com, hdp3.b.xuzq.com")

        conf.set(TableInputFormat.INPUT_TABLE, "public_opinion:metadata")

        val scan = new Scan()
        scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("content"))

        conf.set(TableInputFormat.SCAN, Base64.encodeBytes((ProtobufUtil.toScan(scan)).toByteArray))

        val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
          classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
          classOf[org.apache.hadoop.hbase.client.Result])



        //output the data format in each line is :Map(ImmutableBytesWritable, Result)
        val contentRDD = hBaseRDD.map(_._2).map(result => (result.getRow, Bytes.toString(result.getValue("f1".getBytes, "content".getBytes))))

        val splitCountRDD = contentRDD.map(
          rdd =>{
            val regex = new Regex("""([^0-9]*)""")
            val regexeng = new Regex("""([^a-z]+)""")
            (rdd._1, new SplitWord().splitWordMethod(rdd._2).split(" ").map(_.toLowerCase)
              .filter(token => regex.pattern.matcher(token).matches)
              .filter(token => regexeng.pattern.matcher(token).matches)
              .filter(token => token.size >= 2).toList)
          }
        )
        val sqlContext = new org.apache.spark.sql.SQLContext(rdd.sparkContext)
        import sqlContext.implicits._

        val wordsDataFrame = splitCountRDD.map(record => Record(record._1, record._2, null)).toDF()
        wordsDataFrame.registerTempTable("words")

        val resultMap = new HotWordsExtraction().HotWordsExtractionModel(wordsDataFrame, Math.pow(2, 20).toInt, 20)

        resultMap.foreach(
          message =>{
            message._1.foreach(
              text => {
                var tmp = Integer.toHexString(text & 0xFF)
                if(tmp.size == 1){
                  tmp = "0" + tmp
                }
                print(tmp + " ")
              }
            )
            println(message._2)
          }
        )

        wordsDataFrame.show()
        println("HBaseRDDCount:" + hBaseRDD.count)
        println("dataFrameCount: " + wordsDataFrame.count)
      }
    )*/
    ssc.start()
    ssc.awaitTermination()
  }

  /*case class Record(label: Array[Byte], content: List[String], keyWord: String)

  /** Lazily instantiated singleton instance of SQLContext */
  object SQLContextSingleton {
    @transient private var instance: SQLContext = _
    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }*/

}
