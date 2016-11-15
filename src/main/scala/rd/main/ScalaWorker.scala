package rd.main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.bson.Document

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.clustering.KMeans
import java.util.Arrays

/**
 * Spark K-Means cluster maker - uses Mongo Spark Connector
 */
object ScalaWorker {
  val numClusters = 2;
  val numIteration = 200;
  val collection = "lifetracks";
  def main(args: Array[String]) {

    //Define Spark Cluster including properties for Mongo-Spark Connector
    val conf: SparkConf = new SparkConf();
    conf.setAppName("GeoLife").setMaster("local[*]").set("spark.mongodb.input.uri", "mongodb://localhost/Geo." + collection)
    val ctx: SparkContext = new SparkContext(conf);
    val readConf = ReadConfig(Map("collection" -> collection), Some(ReadConfig(ctx)));

    //Load and cache data from Mongo - can take some time.
    val data = MongoSpark.load(ctx, readConf).map { x => process(x) }.cache();
    
    //Number of clusters
    val clus = 2000

    //Train K-Means
    val model = KMeans.train(data, clus, numIteration);

    //Compute cost
    val WSSE = model.computeCost(data);
    println(clus + " , " + WSSE);
    
    //Write out the cluster centres to the console
    System.out.println(model.clusterCenters.foreach { x => System.out.println(x) })
    
    //Save the K-Means model
    model.save(ctx, "km.model")

  }

  def process(doc: Document): Vector = {
    val loc: Document = doc.get("loc").asInstanceOf[Document]
    val coord = loc.get("coordinates").asInstanceOf[java.util.ArrayList[Double]]

    Vectors.dense(coord.get(0), coord.get(1))

  }
}



