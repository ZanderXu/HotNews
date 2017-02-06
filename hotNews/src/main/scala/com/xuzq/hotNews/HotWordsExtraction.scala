package com.xuzq.hotNews

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

/**
  * Created by xuzq on 16-4-15.
  */
class HotWordsExtraction {

  def nonNegativeMod_sel(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def HotWordsExtractionModel(rawdata: DataFrame, NumFeatures: Int, KeywordsNums: Int): Map[Array[Byte], String] = {

    rawdata.show(false)


    val wordsdata = rawdata.select("content").collect()

    val matrixdata = new ArrayBuffer[mutable.HashMap[Int,String]]

    for (i <- 0 to wordsdata.length - 1) {
      val rel = wordsdata(i).getAs[List[String]](0).toArray
      val wordtohash = mutable.HashMap.empty[Int, String]
      for (j <- 0 to rel.length - 1)
        {
          wordtohash += (nonNegativeMod_sel(rel(j).##, NumFeatures) -> rel(j))
        }
      matrixdata.insert(i,wordtohash)
    }
    val hashingTF = new HashingTF()
      .setInputCol("content").setOutputCol("rawFeatures").setNumFeatures(NumFeatures)

    val featurizedData = hashingTF.transform(rawdata)


    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("featuresed")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    rescaledData.show(false)

    val coldata = rescaledData.select("featuresed")
    coldata.show(false)
   // coldata.rdd.map(v => println(v)).foreach(println)
   // coldata.rdd.map(v => v.get(0)).map(v => v.asInstanceOf[SparseVector]).map(v => v.toSparse).map(v => println(v))take(1)

    /*val exdata = coldata.map(
      v => {
        case Row(v : SparseVector) => {
          v.toSparse
        }
      }
    ).collect()*/
    //val exdata = coldata.map { case Row(v: Vector) => v.toSparse }.collect()
    val exdata = coldata.rdd.map(v => v.asInstanceOf[SparseVector]).map(v => v.toSparse).collect()

    println(exdata(0))

    val spliteddata = rawdata.select("content").collect()

    val labeldata = rawdata.select("label").collect()

    var i = 0
    var nums = KeywordsNums
    var myArray: Array[Int] = new Array[Int](nums)
    var hotwordperdoc: ArrayBuffer[String] = new ArrayBuffer[String]
    val hotwordperdocmap = new scala.collection.mutable.HashMap[Array[Byte], String]


    var hotwordsize = 0
    val rowsize = coldata.count()
    while (i < rowsize) {
      val size = exdata(i).indices.size

      if (nums < size) nums = size
      var n = 0

      val dinices = exdata(i).indices
      val rowdata = spliteddata(i).getAs[List[String]](0).toArray

      val rowdatalabel = labeldata(i).getAs[Array[Byte]](0)


      var wordindexmap = mutable.HashMap.empty[Int, Double]
      for (idx <- 0 to size - 1)
        wordindexmap += (dinices(idx) -> exdata(i).values(idx))

      val sortmap = LinkedHashMap(wordindexmap.toSeq.sortWith(_._2 > _._2): _*)

        hotwordperdoc.insert(i, (matrixdata(i).getOrElse(sortmap.keySet.toList(0), "0")))
        hotwordsize = hotwordsize + 1
        n = n + 1
        i = i + 1
      }

    var resultsmap = Map.empty[Array[Byte], String]

    for (doc <- 0 to coldata.count().toInt - 1) {
      resultsmap += (labeldata(doc).getAs[Array[Byte]](0) -> hotwordperdoc(doc))
    }
    resultsmap
  }

}
