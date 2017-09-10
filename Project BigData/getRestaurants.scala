package com.axb154830.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import java.util.HashMap
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import scala.collection.mutable.ListBuffer

object getRestaurants {
  def main(args: Array[String]) = {

    //setting up context and reading the file

    var city = "pittsburgh"
    var residingState = "PA"
    residingState = residingState.toLowerCase
    city = city.toLowerCase
   
    val conf = new SparkConf().setAppName("Ml Test").setMaster("local[*]").set("spark.executor.memory", "20g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    var df = sqlContext.read
      .format("com.databricks.spark.csv")
      .load("C:\\Users\\Abhitej\\Documents\\Masters\\sem3\\BigData\\project\\yelp_dataset_challenge_academic_dataset\\yelp_academic_dataset_business.csv")

    var rows: RDD[Row] = df.rdd

    var restaurantList = scala.collection.mutable.MutableList[String]()
     var ResName = new HashMap[String, String]()
     
    var ResNameRat = scala.collection.mutable.Map[String, String]()
    rows.collect().foreach {

      i =>

        var lineStringArr = (i.toString()).split(",")
        //println(i.toString())
        //println(lineStringArr.size)
        val categories = lineStringArr(19).toLowerCase
        val bid = lineStringArr(14)
        val name = lineStringArr(22)
        val restaurantState = lineStringArr(37).toLowerCase
        val restaurantCity = lineStringArr(0).toLowerCase
        
        
        //println(categories+" "+restaurantCity+" "+bid+" "+restaurantState )

       
          
        
        
        //println(bid.toString+" ")

        if (categories.contains("restaurant")  && restaurantCity.contains(city) && restaurantState.contains(residingState)) {
          restaurantList += bid
         ResName.put(bid, name)
         
         if(!lineStringArr(1).equals("stars"))
          {
           val rat = lineStringArr(1).toString()
           ResNameRat.put(name, rat)
          }
        }
    }
    
    df = sqlContext.read
      .format("com.databricks.spark.csv")
      .load("C:\\Users\\Abhitej\\Documents\\Masters\\sem3\\BigData\\project\\yelp_dataset_challenge_academic_dataset\\yelp_academic_dataset_tip.csv")

     val TipRows:RDD[Row] = df.rdd.filter(x => restaurantList.contains(x(2)))
    
     var TipRowRelevantData = scala.collection.mutable.MutableList[String]()
     
     TipRows.collect().foreach {
          i =>
        val lineStringArr = i.toString.split(",")

        val tip_info = lineStringArr(1).trim
        val bid = lineStringArr(2).trim
        val resName = ResName.get(bid)
        var resRat=ResNameRat.get(resName).toString()
        
        resRat = resRat.replace("Some(", "").replace(")", "")
        print("ResRat: "+resRat)
        val tip_String = bid+"::"+resName+"::"+tip_info+"::"+resRat
        
        TipRowRelevantData+=tip_String
    }
     
    //TipRowRelevantData.foreach(println)
    println("TipRowRelData Count = "+TipRowRelevantData.size)
     sc.parallelize(TipRowRelevantData, 1).saveAsTextFile("../TipsNew.dat")  
    
  }
}