val review = sc.textFile("inputReview") 
val reviewData = review.map(line => line.split("\\::")).map(line => (line(2), line(3).toDouble))
val result =  reviewData.map { case (key, value) => (key, (value, 1)) }.reduceByKey { case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2)}.mapValues {case (value, count) =>  value.toDouble / count .toDouble}
val topTen = result.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2))
val topTenParallelize = sc.parallelize(topTen)  

val business = sc.textFile("inputBusiness")
val businessData = business.map(line => line.split("\\::")).map(line => (line(0), (line(1)+"\t\t"+line(2)).toString)) 
 
val res = businessData.join(topTenParallelize).distinct()
res.foreach(println)