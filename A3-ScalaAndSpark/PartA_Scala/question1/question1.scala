import scala.io.Source
import scala.util.control._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

val movieData=sc.textFile("sxh164530/movies.dat").map(line=>line.split("::",2)).map(line=>(line(0)->line(1))).collect().toMap;
    val data = sc.textFile("sxh164530/itemusermat")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()


    val numClusters = 10
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val vectors11 = parsedData.collect().toList

    var count=0;
    val loop = new Breaks
    var test = 0;
    loop.breakable {
      for (i <- vectors11) {
        val j = clusters.predict(i);

        if (test == j) {
          val array=i.toString.split(",");
          val required=array(0).replace("[","").replace(".","").toInt/10;

          val valueToBePrinted=movieData(required.toString).replace("::",",");
          System.out.println("cluster:"+j);
          System.out.println(required+" " +valueToBePrinted);
          count += 1;
          if (count == 5) {
            test += 1;
            count=0;
          }
        }
        if(test>10)
        {
          loop.break();
        }
      }
    }

  
