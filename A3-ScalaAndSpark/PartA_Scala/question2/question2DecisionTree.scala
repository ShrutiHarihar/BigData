import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
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
    val splits = parsedData.randomSplit(Array(0.6, 0.4),seed=11L)
    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 8
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testError = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
        println("Accuracy = "+ (1-testError)*100)
	
	
	

	
