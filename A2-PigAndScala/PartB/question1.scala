val business= sc.textFile("inputBusiness")
val review= sc.textFile("inputReview")

val businessData = business.map(line=>line.split("\\::")).filter(line=>line(1).contains("Stanford")).map(line=>(line(0)))

val reviewData = review.map(line=>line.split("\\::")).map(line=>(line(1),line(2),line(3)))

val businessDF = businessData.toDF("businessid")
val reviewDF = reviewData.toDF("userid","businessid","rating")

val output = reviewDF.join(businessDF,"businessid").distinct().select("userid","rating")
    
output.foreach(println)