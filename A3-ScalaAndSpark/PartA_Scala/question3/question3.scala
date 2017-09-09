import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

    val data = sc.textFile("/sxh164530/ratings.dat")
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val ratingsTrain = training.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val ratingsTest = training.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratingsTrain, rank, numIterations, 0.01)
    val usersProductsTrain = ratingsTrain.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val usersProductsTest = ratingsTest.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions =
      model.predict(usersProductsTest).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = ratingsTest.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
    println("Accuracy = " + (1-MSE)*100)

