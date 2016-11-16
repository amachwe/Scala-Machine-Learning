package rd.main

import java.io.File
import java.io.FileFilter
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Random

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.bson.Document

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import java.util.Date

case class DataToLoad(fileList: ListBuffer[File], numLoaders: Int)
case class FileProcessed(numEntriesLoaded: Int)

/**
 * Geo Trails Data Loader, uses Actors
 * Data and information about it can be found here: https://www.microsoft.com/en-us/download/details.aspx?id=52367
 */
object GeoTrailsLoader {

  val MONGO_HOST = "localhost"
  val MONGO_PORT = 27017
  val GEODATA_PATH = "D:/Geolife Trajectories 1.3/Data"
  val COLLECTION_NAME = "lifetracks"
  val DB_NAME = "Geo"
  val NUM_LOADERS = 4

  val PLT_FILETYPE_FILTER = new FileFilter() {
    def accept(file: File): Boolean = {
      return (file.getName().endsWith(".plt") || file.isDirectory())
    }
  }

  /**
   * Recursive load all files matching filter condition
   * @param Root Directory
   * @param Flattened list of files
   * @param PLT file filter
   */
  def load(rootDir: File, list: ListBuffer[File], filter: FileFilter): Unit =
    {
      if (rootDir != null && rootDir.exists() && rootDir.isDirectory()) {
        rootDir.listFiles(filter).toList.foreach { f => { load(f, list, filter) } }
      }
      if (rootDir.isFile()) {
        list.append(rootDir);
      }

    }

  def main(args: Array[String]) {
    //Collect all data files
    val list = new ListBuffer[File]()
    load(new File(GEODATA_PATH), list, PLT_FILETYPE_FILTER)

    val totalFiles = list.size
    println(s"Total Files to load: ${totalFiles}")

    //Init Mongo and get Destination Collection
    val client = new MongoClient(MONGO_HOST, MONGO_PORT)
    val tracks = client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
    tracks.drop();

    //Init Actors
    val mainActor: ActorRef = ActorSystem("MainActor").actorOf(Props(new MainActor(tracks)))
    mainActor ! DataToLoad(list, NUM_LOADERS)
  }
}

object MongoLoaderActor {

  /**
   * Execute the load: extract from file, load into doc structure and then add to list for bulk writing
   */
  def execute(fileName: String, mongoCollection: MongoCollection[Document]): Int = {
    val list = new ArrayList[Document]();
    Source.fromFile(fileName).getLines().filter { str => str.length() > 31 }.map { str => extract(str) }.map { row => load(row, fileName) }.foreach { x => list.add(x) };
    mongoCollection.insertMany(list);
    return list.size()
  }

  def extract(str: String): Array[String] = {
    val row: Array[String] = str.split(",");
    if (row.length == 7) {
      return row;
    } else {
      return null;
    }
  }

  //Load the data using Geocoded point (WGS 84)
  def load(row: Array[String], fileName: String): Document = {
    val dateTimeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //{ type: "Point", coordinates: [ long, lat ] } WGS 84
    val point = new Document();
    point.put("type", "Point");

    val coord: ArrayList[Double] = new ArrayList[Double];
    coord.add(row(1).toDouble);
    coord.add(row(0).toDouble);
    point.put("coordinates", coord);

    val doc = new Document();
    doc.put("source", fileName);
    doc.put("loc", point);
    doc.put("alt_ft", row(3).toDouble);
    val dateTime = (row(5) + " " + row(6)).trim();
    try {
      doc.put("date_time", dateTimeFormat.parse(dateTime))
    } catch {
      case e: Exception => {
        System.err.println("Error Processing: " + dateTime);
        e.printStackTrace();
      }
    }
    return doc
  }
}

class MainActor(mongoCollection: MongoCollection[Document]) extends Actor {
  var totalFilesToProcess: Int = 0
  var totalEntriesLoaded: Long = 0L
  var totalFilesProcessed: Long = 0L

  def receive = {
    case DataToLoad(fileList: ListBuffer[File], numLoaders: Int) => {
      totalFilesToProcess = fileList.size;
      if (totalFilesToProcess > 0) {
        val loaders = createWorkers(numLoaders, mongoCollection)
        fileList.zipWithIndex.foreach(e => loaders(e._2 % numLoaders) ! e._1.toString())
      }
    }
    case FileProcessed(numEntriesLoaded: Int) => {
      totalEntriesLoaded += numEntriesLoaded
      totalFilesProcessed += 1L
      println(s"Processed ${totalFilesProcessed}/${totalFilesToProcess} files")
      if (totalFilesProcessed == totalFilesToProcess) {
        println(s"Completed processing all ${totalFilesToProcess} files")
        context.system.terminate()
      }
    }
  }

  private def createWorkers(numActors: Int, mongoCollection: MongoCollection[Document]) = {
    for (i <- 0 until numActors) yield context.actorOf(Props(new MongoLoaderActor(mongoCollection)), name = s"loader-${i}")
  }

}

/**
 * File Process Actor - MongoCollection to send the data to and Logger Actor
 */
class MongoLoaderActor(mongoCollection: MongoCollection[Document]) extends Actor {

  def receive = {
    case (fileName: String) =>
      {
        val count: Int = MongoLoaderActor.execute(fileName, mongoCollection)
        context.parent ! FileProcessed(count);
      }
  }
}