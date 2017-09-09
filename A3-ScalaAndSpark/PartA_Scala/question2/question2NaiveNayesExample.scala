package org.apache.spark.examples.mllib
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

    val data=sc.textFile("/sxh164530/glass.data")
    val parsedData=data.map { line =>
      val parts = line.split(',')
      var count=0;
      val builder=StringBuilder.newBuilder
      for(i<-parts){
        if(count!=parts.length-1 && count!=0){
          if(count!=parts.length-2){
            builder.append(i).append(",")}
          else if(count==parts.length-2){
            builder.append(i)
          }
        }
        count+=1
      }
      LabeledPoint(parts(parts.length-1).toDouble, Vectors.dense(builder.toString().split(',').map(_.toDouble)))
    }
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 100.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
	
	
		
