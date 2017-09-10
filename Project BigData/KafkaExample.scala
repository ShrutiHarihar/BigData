/**
  * Created by Ravindra2502 on 12/10/2016.
  */
import java.util

import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._
import stemmer.Stemmer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark._
import java.util.Date
import javax.sql.RowSetMetaData

import org.apache.spark
import org.apache.spark.streaming.dstream.DStream

import scala.util.control.Breaks
import scala.util.control.Breaks._

object KafkaExample {
  def main(args: Array[String]) {
    val stem = new Stemmer
    // val matcher = "(\\ba\\b|\\bable\\b|\\babout\\b|\\bacross\\b|\\bafter\\b|\\ball\\b|\\balmost\\b|\\balso\\b|\\bam\\b|\\bamong\\b|\\ban\\b|\\band\\b|\\bany\\b|\\bare\\b|\\bas\\b|\\bat\\b|\\bbe\\b|\\bbecause\\b|\\bbeen\\b|\\bbut\\b|\\bby\\b|\\bcan\\b|\\bcannot\\b|\\bcould\\b|\\bdear\\b|\\bdid\\b|\\bdo\\b|\\bdoes\\b|\\beither\\b|\\belse\\b|\\bever\\b|\\bevery\\b|\\bfor\\b|\\bfrom\\b|\\bget\\b|\\bgot\\b|\\bhad\\b|\\bhas\\b|\\bhave\\b|\\bhe\\b|\\bher\\b|\\bhers\\b|\\bhim\\b|\\bhis\\b|\\bhow\\b|\\bhowever\\b|\\bi\\b|\\bif\\b|\\bin\\b|\\binto\\b|\\bis\\b|\\bit\\b|\\bits\\b|\\bjust\\b|\\bleast\\b|\\blet\\b|\\blike\\b|\\blikely\\b|\\bmay\\b|\\bme\\b|\\bmight\\b|\\bmost\\b|\\bmust\\b|\\bmy\\b|\\bneither\\b|\\bno\\b|\\bnor\\b|\\bnot\\b|\\bof\\b|\\boff\\b|\\boften\\b|\\bon\\b|\\bonly\\b|\\bor\\b|\\bother\\b|\\bour\\b|\\bown\\b|\\brather\\b|\\bsaid\\b|\\bsay\\b|\\bsays\\b|\\bshe\\b|\\bshould\\b|\\bsince\\b|\\bso\\b|\\bsome\\b|\\bthan\\b|\\bthat\\b|\\bthe\\b|\\btheir\\b|\\bthem\\b|\\bthen\\b|\\bthere\\b|\\bthese\\b|\\bthey\\b|\\bthis\\b|\\btis\\b|\\bto\\b|\\btoo\\b|\\btwas\\b|\\bus\\b|\\bwants\\b|\\bwas\\b|\\bwe\\b|\\bwere\\b|\\bwhat\\b|\\bwhen\\b|\\bwhere\\b|\\bwhich\\b|\\bwhile\\b|\\bwho\\b|\\bwhom\\b|\\bwhy\\b|\\bwill\\b|\\bwith\\b|\\bwould\\b|\\byet\\b|\\byou\\b|\\byour\\b)".r

    val bigramTest = "good pizza"
    val (zkQuorum, group, topics, numThreads) = ("localhost", "localhost", "project", "20")
    val sparkConf = new SparkConf().setMaster("local[*]").setSparkHome("/usr/local/spark").setAppName("yelp").set("spark.driver.allowMultipleContexts","true")
    //spark.driver.allowMultipleContexts=true
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val stopWords = "(\\ba\\b|\\bable\\b|\\babout\\b|\\bacross\\b|\\bafter\\b|\\ball\\b|\\balmost\\b|\\balso\\b|\\bam\\b|\\bamong\\b|\\ban\\b|\\band\\b|\\bany\\b|\\bare\\b|\\bas\\b|\\bat\\b|\\bbe\\b|\\bbecause\\b|\\bbeen\\b|\\bbut\\b|\\bby\\b|\\bcan\\b|\\bcannot\\b|\\bcould\\b|\\bdear\\b|\\bdid\\b|\\bdo\\b|\\bdoes\\b|\\beither\\b|\\belse\\b|\\bever\\b|\\bevery\\b|\\bfor\\b|\\bfrom\\b|\\bget\\b|\\bgot\\b|\\bhad\\b|\\bhas\\b|\\bhave\\b|\\bhe\\b|\\bher\\b|\\bhers\\b|\\bhim\\b|\\bhis\\b|\\bhow\\b|\\bhowever\\b|\\bi\\b|\\bif\\b|\\bin\\b|\\binto\\b|\\bis\\b|\\bit\\b|\\bits\\b|\\bjust\\b|\\bleast\\b|\\blet\\b|\\blike\\b|\\blikely\\b|\\bmay\\b|\\bme\\b|\\bmight\\b|\\bmost\\b|\\bmust\\b|\\bmy\\b|\\bneither\\b|\\bno\\b|\\bnor\\b|\\bnot\\b|\\bof\\b|\\boff\\b|\\boften\\b|\\bon\\b|\\bonly\\b|\\bor\\b|\\bother\\b|\\bour\\b|\\bown\\b|\\brather\\b|\\bsaid\\b|\\bsay\\b|\\bsays\\b|\\bshe\\b|\\bshould\\b|\\bsince\\b|\\bso\\b|\\bsome\\b|\\bthan\\b|\\bthat\\b|\\bthe\\b|\\btheir\\b|\\bthem\\b|\\bthen\\b|\\bthere\\b|\\bthese\\b|\\bthey\\b|\\bthis\\b|\\btis\\b|\\bto\\b|\\btoo\\b|\\btwas\\b|\\bus\\b|\\bwants\\b|\\bwas\\b|\\bwe\\b|\\bwere\\b|\\bwhat\\b|\\bwhen\\b|\\bwhere\\b|\\bwhich\\b|\\bwhile\\b|\\bwho\\b|\\bwhom\\b|\\bwhy\\b|\\bwill\\b|\\bwith\\b|\\bwould\\b|\\byet\\b|\\byou\\b|\\byour\\b)".r

    //ssc.checkpoint("checkpoint")

    case class Auction( business_id: String, restaurant_id: String, TipData: String)

    val topic = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines: DStream[String] = KafkaUtils.createStream(ssc, zkQuorum, group, topic).map(_._2)

    val lineRows: DStream[Array[String]] = lines.map(x => x.split("\\^"))

   // val idWithTip = lineRows.map(line =>line.split(",")).map(line=>(line(0)->line(2)))


    //val  tipDataHmap =  new util.HashMap[String, StringBuilder]()
    var tipDataMap = scala.collection.mutable.Map[String,StringBuilder]()
    var restRatMap = new java.util.HashMap[String,Double]()
    var restNameMap = new java.util.HashMap[String,String]()

    var matchRestRatMap = new java.util.HashMap[String, Double]()

    var matchrestIdsRest = new java.util.HashSet[String]()
    var fileWritten = false

    var top1 = -1
    var top2 = -1
    var top3 = -1
    var top4 = -1
    var top5 = -1

    var top1Rest =""
    var top2Rest =""
    var top3Rest =""
    var top4Rest =""
    var top5Rest =""




  lines.foreachRDD{rdd =>
        rdd.collect().foreach {
          record =>
            val recordArr = record.split("::")
            if (recordArr.size > 3) {
              if (tipDataMap.keySet.contains(recordArr(0))) {
                var arrBuilder = new StringBuilder
                arrBuilder = tipDataMap(recordArr(0))
                arrBuilder.append(recordArr(2))
                var rat = recordArr(3).toDouble
                tipDataMap.put(recordArr(0), arrBuilder)
                restRatMap.put(recordArr(0),rat)
                restNameMap.put(recordArr(0),recordArr(1))

                //print("TipData Size: " + tipDataMap.size)
                //println("Row "+recordArr(0))
              }
              else {
                var arrBuilder = new StringBuilder
                arrBuilder.append(recordArr(2))
                tipDataMap.put(recordArr(0), arrBuilder)
                var rat = recordArr(3).toDouble
                restRatMap.put(recordArr(0),rat)
                restNameMap.put(recordArr(0),recordArr(1))
                /*
                if (tipDataMap.size > 1200) {
                  if(!fileWritten) {
                    sc.parallelize(tipDataList, 1).saveAsTextFile("../StreamOutput.dat")
                    fileWritten=true
                  }
                  */
              }
                // println(recordArr(0))
              }
              //println("RowData " + record)
            }

          println("TipData Size: "+tipDataMap.size)
          println("Ratings Size: "+restRatMap.size)

          for(restId <- tipDataMap.keySet) {
            var tipData = tipDataMap.get(restId)

            var tipDataRDD = sc.parallelize(tipData.toSeq,1)

            var tokens =tipDataRDD.map{
              // Split each line into substrings by periods
              _.split('.').map{ substrings =>
                // Trim substrings and then tokenize on spaces
                substrings.trim.split(' ').
                  // Remove non-alphanumeric characters, using Shyamendra's
                  // clean replacement technique, and convert to lowercase
                  map{_.replaceAll("""\W""", "").toLowerCase()}.
                  // Find bigrams
                  sliding(2)
              }.
                // Flatten, and map the bigrams to concatenated strings
                flatMap{identity}.map{_.mkString(" ")}.
                // Group the bigrams and count their frequency
                groupBy{identity}.mapValues{_.size}
            }.
              // Reduce to get a global count, then collect
              flatMap{identity}.reduceByKey(_+_)

            //println("Rest ID: "+restId)

            tokens.collect.foreach {
              x =>
                if(x._1.contains(bigramTest))
                  {
                    matchrestIdsRest.add(restId)
                  //println("RestContains Name: "+restNameMap.get(restId))
                    // println("RestContains Rating: "+restRatMap.get(restId))
                  }
            }

          }
          println("Match Restaurants Count: "+matchrestIdsRest.size())
          var nameRatMap:Map[String,Double] = Map()
          for(i<-matchrestIdsRest)
            {
              nameRatMap+=(restNameMap.get(i)->restRatMap.get(i))
              //println("MatchRestContains Name: "+restNameMap.get(i))
              //println("MatchRestContains Rating: "+restRatMap.get(i))
            }
            import scala.collection.immutable.ListMap

            var sortedMap=ListMap(nameRatMap.toSeq.sortWith(_._2 > _._2):_*)
            for((k,v)<-sortedMap)
              println("Restaurant Name: "+k+" Restaurant rating: "+v)
          tipDataMap= scala.collection.mutable.Map[String,StringBuilder]()
          restRatMap = new java.util.HashMap[String,Double]()
          matchrestIdsRest = new java.util.HashSet[String]()

        }

    for( i<- tipDataMap.keySet)
      {
        print("Keys and Values: ")
        println(i.toString+" :: "+ tipDataMap(i.toString).toString())
      }

    /*
    val arr = scala.collection.mutable.ArrayBuffer.empty[String]

    lineRows.foreachRDD {
      arr ++= _ //you can now put it in an array or d w/e you want with it
    }

    if(arr.size>0) {
      print("FirstLine: " + arr(0))
      println(arr.size)
    }
    */

    /*
    val lineTuples= lines.map{
      _.split("\\^").map { RowData =>

        print("Row: ")
        println(RowData.toString)
      }
    }
*/

    print("The Count: ")
    lineRows.count.print()
    /*
    //lineRows.print()

    for(i<-lineRows) {
      println("line:   ")
      println(i)
      print(lineRows.count().toString.equals("0"))
    }


    if(!lineRows.count().toString.equals("0"))
      {

      }
*/

    /*
      val tokens = lineRows.map {

        _.split('.').map { substrings =>
          substrings.trim.split(' ').
            map {
              _.replaceAll("""\W""", "").toLowerCase()
            }.map(a => stopWords.replaceAllIn(a, "")).map(v => {
            stem.add(v.toArray, v.length)
            stem.stem()
            stem.toString
          }).

            sliding(2)
        }.

          flatMap {
            identity
          }.map {
          _.mkString(" ")
        }.

          groupBy {
            identity
          }.mapValues {
          _.size
        }
      }.flatMap {
        identity
      }.reduceByKey(_ + _)

      tokens.print()
    */

      //words.saveAsTextFiles("../sample.dat")
      ssc.start()
      ssc.awaitTermination()


  }
}