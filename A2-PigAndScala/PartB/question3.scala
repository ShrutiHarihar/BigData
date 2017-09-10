val graph = sc.textFile("inputGraph")
val graphData = graph.map(line => line.split("\t")).map(line => (line(1), line(2).trim.toInt)).reduceByKey((count1, count2) => count1+count2)
graphData.foreach(println)