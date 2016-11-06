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

/**
 * Geo Trails Data Loader, uses Actors
 * Data and information about it can be found here: https://www.microsoft.com/en-us/download/details.aspx?id=52367
 */
object GeoTrailsLoader {

  val host = "localhost"
  val port = 27017

  /**
   * PLT type file filter
   */
  val PLT_FILE_DIR_FILTER = new FileFilter() {
    def accept(file: File): Boolean = {

      if (file.getName().endsWith(".plt") || file.isDirectory()) {
        true
      } else {
        false
      }
    }
  };

  /**
   * Recursive load all files matching filter condition
   * @param Root Directory
   * @param Flattened list of files
   * @param File filter
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

  /**
   * Main entry point for the Loader App
   */
  def main(args: Array[String]) {

    val rnd = new Random();
    
    //Init Actors
    val system = ActorSystem("FileTask");

    //Collect all data files
    val list = new ListBuffer[File]()
    load(new File("d:\\ml stats\\Geolife Trajectories 1.3\\Data"), list, PLT_FILE_DIR_FILTER)

    //Init Mongo and get Destination Collection
    val client = new MongoClient(host, port)
    val tracks = client.getDatabase("Geo").getCollection("lifetracks");

    //Drop if any
    tracks.drop();

    val totalFiles = list.size;
    val logger: ActorRef = system.actorOf(Props(new LoggingActor(totalFiles)));

    val fpa = List(system.actorOf(Props(new FileProcess(tracks, logger)), "fileprocess1"),
      system.actorOf(Props(new FileProcess(tracks, logger)), "fileprocess2"),
      system.actorOf(Props(new FileProcess(tracks, logger)), "fileprocess3"),
      system.actorOf(Props(new FileProcess(tracks, logger)), "fileprocess4"));

    System.out.println("Total Files: " + totalFiles);

    list.foreach { x => fpa(rnd.nextInt(fpa.size)) ! x.toString() }

    fpa.foreach { x => x ! FileProcess.PoisonPill };

  }

}

object FileProcess {
  val fileNameSet: java.util.concurrent.ConcurrentSkipListSet[String] = new java.util.concurrent.ConcurrentSkipListSet[String];
  val PoisonPill = new Object();

}
/**
 * File Process Actor - MongoCollection to send the data to and Logger Actor
 */
class FileProcess(mongoCollection: MongoCollection[Document], logger: ActorRef) extends Actor {

  val dateTimeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  def receive = {
    case (fileName: String) =>
      {
        execute(fileName);
        FileProcess.fileNameSet.add(fileName);

        logger ! FileProcess.fileNameSet.size;

      }
    case (obj: Object) => {
      if (obj == FileProcess.PoisonPill) {
        System.out.println("Received Poison Pill");
        context.stop(self);
      }
    }
  }

  /**
   * Execute the load: extract from file, load into doc structure and then add to list for bulk writing
   */
  def execute(fileName: String) {

    val list = new ArrayList[Document]();
    Source.fromFile(fileName).getLines().filter { str => str.length() > 31 }.map { str => extract(str) }.map { row => load(row, fileName) }.foreach { x => list.add(x) };

    mongoCollection.insertMany(list);
  }

  def extract(str: String): Array[String] = {
    val row: Array[String] = str.split(",");
    if (row.length == 7) {
      return row;
    } else {
      return null;
    }
  }

  var count = 0;
  //Load the data using Geocoded point (WGS 84)
  def load(row: Array[String], fileName: String): Document = {
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

      doc.put("date_time", dateTimeFormat.parse(dateTime));
    } catch {

      case e: Exception => {
        System.err.println("Error Processing  > " + dateTime);
        e.printStackTrace(System.err);
      }
    }
    
    doc
  }
}

/**
 * Log Progress - Actor to log progress
 */
class LoggingActor(totalFiles: Integer) extends Actor {
  var lastVal = 0;
  var lastProgress = 0;

  def receive = {
    case pComp: Integer => {
      if (pComp > lastVal) {
        lastVal = pComp;
        val progress = Math.round(lastVal * 100f / totalFiles);
        if (lastProgress < progress) {
          lastProgress = progress;
          System.out.println(lastProgress + ", " + System.currentTimeMillis());
          if (progress == 100) {
            System.out.println("Logging Actor exiting... ");
            context.stop(self);
          }
        }
      }
    }
  }
}