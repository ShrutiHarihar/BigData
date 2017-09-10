package com.axb154830.spark
import java.util

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

/**
 * Created by srinivas on 11/28/16.
 */
object Part1 {

  def main(args: Array[String]) = {

    //setting up context and reading the file

    var user_id = "Fr12lvqUHN6dmMysQ"
    //"Fr12lvqUHN6dE5fJ5mMysQ"
    var type_of_restaurant = "indian"
    var city = "pittsburgh"
    var residingState = "PA"
    residingState = residingState.toLowerCase
    city = city.toLowerCase
    type_of_restaurant = type_of_restaurant.toLowerCase
    //val business_id="5UmKMjUEUNdYWqANhGckJw";
    val conf = new SparkConf().setAppName("Ml Test").setMaster("local[*]").set("spark.executor.memory", "20g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    var df = sqlContext.read
      .format("com.databricks.spark.csv")
      .load("C:\\Users\\Abhitej\\Documents\\Masters\\sem3\\BigData\\project\\yelp_dataset_challenge_academic_dataset\\yelp_academic_dataset_business.csv")

    val rows: RDD[Row] = df.rdd

    var restaurantList = scala.collection.mutable.MutableList[String]()

    //rows.collect().foreach(println)

    var ResName = new util.HashMap[String, String]()
    var ResNameRat = scala.collection.mutable.Map[String, Double]()
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

        if (categories.contains("restaurant") && categories.contains(type_of_restaurant) && restaurantCity.contains(city) && restaurantState.contains(residingState)) {
          restaurantList += bid
          ResName.put(bid, name)
          
          
          if(!lineStringArr(1).equals("stars"))
          {
           val rat = lineStringArr(1).toDouble
           ResNameRat.put(name, rat)
          }
        }

    }

    //println("Restaurant ids: ")
    //restaurantList.foreach(println)

    df = sqlContext.read
      .format("com.databricks.spark.csv")
      .load("C:\\Users\\Abhitej\\Documents\\yelp_academic_dataset_review.csv")

    var userReviewed = false

    //val reviewsRows: RDD[Row] = df.rdd
    val reviewsRows: RDD[Row] = df.rdd.filter(x => restaurantList.contains(x(3)))

    //reviewsRows.collect().foreach(println)

    var hp = new util.HashMap[String, String]()

    reviewsRows.collect().foreach {

      i =>
        val lineStringArr = i.toString.split(",")

        val uid = lineStringArr(0).trim.replace("[", "")
        val review_bid = lineStringArr(3).trim
        val rating = lineStringArr(5).toFloat
        val dateReviewed = lineStringArr(6)

        // println("bussiness id: "+review_bid)
        //println("Review bid: "+review_bid)
        //println("User id: "+uid)
        if (uid.contains(user_id) && restaurantList.contains(review_bid)) {
          userReviewed = true
        }

        if (restaurantList.contains(review_bid)) {
          val dt1 = new SimpleDateFormat("yyyy-mm-dd")
          var date1 = dt1.parse(dateReviewed)
          var dateInMilli = date1.getTime
          val uid_bid = uid + "|" + review_bid
          val rating_date = rating + "|" + dateInMilli
          //println("uid_bid: "+uid_bid);
          if (hp.containsKey(uid_bid)) {
            //println("checkDate: "+hp.get(uid_bid)); 
            var existingDateArray = hp.get(uid_bid).split("\\|")
            //println("checkDate: "+existingDateArray(1)); 
            var existingDate = existingDateArray(1).toLong
            //println("Existing time: "+existingDate)
            //println("User_Business id: "+uid_bid)
            if (dateInMilli > existingDate) {
              hp.put(uid_bid, rating_date)
            }
          } else {
            hp.put(uid_bid, rating_date)
          }

        }

    }

    //println("User Reviewed: "+userReviewed)
    //println("No of reviews"+hp.size())

    var useridToNum = new util.HashMap[String, Int]()
    var businessidToNum = new util.HashMap[String, Int]()
    var usercount = 2

    for (i <- hp.keySet().toArray()) {
      var tempArray = i.toString().split("\\|")
      var tempuid = tempArray(0)
      var tempbid = tempArray(1)

      if (tempuid.equals(user_id)) {
        useridToNum.put(tempuid, 1)
      } else {
        useridToNum.put(tempuid, usercount)
      }
      businessidToNum.put(tempbid, usercount)
      usercount += 1
    }

    var ALSList = scala.collection.mutable.MutableList[String]()
    for (i <- hp.keySet().toArray()) {
      var tempArray = i.toString().split("\\|")
      var temp = useridToNum.get(tempArray(0)) + "::" + businessidToNum.get(tempArray(1))
      var valueArray = hp.get(i).toString().split("\\|")
      temp = temp + "::" + valueArray(0) + "::" + valueArray(1)
      ALSList += temp
    }

    //println("useridToNum" + useridToNum.get(user_id))

    //ALSList.foreach(println)

    sc.parallelize(ALSList, 1).saveAsTextFile("../RatingsNew.dat")

    //  val ratings = new Rating(ALSList.map(_.split("::") match { case Array(userID, businessID, ratings,timestamp) =>
    // Rating(useridToNum.get(userID), businessidToNum.get(businessID), ratings.toDouble)}))

    var testHM = scala.collection.mutable.MutableList[String]()
    for (i <- businessidToNum.keySet().toArray()) {
      var temp = "1" + "::" + businessidToNum.get(i) + "::" + "0"
      testHM += temp
    }
    sc.parallelize(testHM, 1).saveAsTextFile("../TestRatings.dat")

    if (userReviewed == true) {
      var data = sc.textFile("../RatingsNew.dat/part-00000")
      val ratings = data.map(_.split("::") match {
        case Array(userID, businessID, ratings, timestamp) =>
          Rating(userID.toInt, businessID.toInt, ratings.toDouble)
      })
      data = sc.textFile("../TestRatings.dat/part-00000")
      val testing = data.map(_.split("::") match {
        case Array(userID, businessID, ratings) =>
          Rating(userID.toInt, businessID.toInt, ratings.toDouble)
      })

      val rank = 5
      val numIterations = 10
      val model = ALS.train(ratings, rank, numIterations, 0.01)

      // Evaluate the model on rating data
      val usersProducts = testing.map {
        case Rating(user, product, ratings) =>
          (user, product)
      }
      val predictions =
        model.predict(usersProducts).map {
          case Rating(user, product, rate) =>
            ((user, product), rate)
        }

      var l = predictions.sortBy(-_._2)

      //l.collect().foreach(println)

      l = sc.parallelize(l.take(5), 1)

      var Top5Rest = scala.collection.mutable.MutableList[String]()
            var Top5RestRat = scala.collection.mutable.Map[String,String]()
      //scala.collection.mutable.MutableList[String]()
       var ratMax = java.lang.Double.parseDouble("0")
      for (i <- l.collect().toArray) {
        var temp = i._1._2.toString()
        // println(temp)
        var tempRat=i._2
        if(ratMax<i._2)
        {
          ratMax = tempRat
        }
        
        Top5Rest += temp
        Top5RestRat.put(temp, tempRat.toString())
        // println(Top5Rest.size)
      }
      //println("Best rating: "+ratMax)
println("Top 5 Restaurants:")
      for (j <- businessidToNum.keySet().toArray()) {
        val strValue = businessidToNum.get(j).toString()
        if (Top5Rest.contains(strValue)) {

          //normalize
          var currentRat = java.lang.Double.parseDouble(Top5RestRat.get(strValue).toString().replace("Some(", "").replace(")", ""))
          currentRat = currentRat * 5/(ratMax+1)
          
          println(ResName.get(j.toString())+" "+currentRat)
        }
      }

    }
    else{
      var ratMapSeq=ResNameRat.toSeq.sortBy(-_._2).take(5) 
      
      ratMapSeq.foreach{
        x=>
          
          println(x._1+" "+x._2)
      }
      
    }

  }
}