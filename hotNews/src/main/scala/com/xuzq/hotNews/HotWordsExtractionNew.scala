package com.xuzq.hotNews

import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by xuzq on 16-9-9.
  */
class HotWordsExtractionNew {

  def HotWordsExtractionModel(rawdata: DataFrame, NumFeatures: Int, KeywordsNums: Int): Map[Array[Byte], String] = {

    val hashingTF = new HashingTF()
      .setInputCol("content").setOutputCol("rawFeatures").setNumFeatures(NumFeatures)
    val featurizedData = hashingTF.transform(rawdata)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val resultArray = rescaledData.select("label", "content", "features").rdd
      .map(v => (v(0), v(1), v(2).asInstanceOf[SparseVector].indices, v(2).asInstanceOf[SparseVector].values))
      .map(
        v => {
          val wordArray = v._2.asInstanceOf[Seq[String]]
          val contextArray = v._3.toList
          val valuesArray = v._4.toList



          contextArray.foreach(print)
          valuesArray.foreach(print)

          var contextIndex = 0
          var valuesIndex = 0.0

          for(i <- 0 until contextArray.length - 1){
            if(valuesIndex < valuesArray(i)){
              contextIndex = contextArray(i)
              valuesIndex = valuesArray(i)
            }
          }

          var word = ""
          val contentToHash = new ContentToHash()

          for(msg <- wordArray.distinct){
            if(contentToHash.getHashCode(msg, NumFeatures) == contextIndex){
              word = msg
            }
          }

          //(v._1, word, contextIndex, valuesIndex)
          (v._1, word)
        }
      ).collect()


    var resultMap = Map.empty[Array[Byte], String]

    for(result <- resultArray){
      resultMap += (result._1.asInstanceOf[Array[Byte]] -> result._2)
    }

    resultMap
  }

}
