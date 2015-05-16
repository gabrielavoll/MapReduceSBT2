/* Gabriela Voll Gracie Liang */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tachyon.worker.WorkerSpaceCounter
import scala.io.Source
import scala.collection.immutable.List
import scala.actors.Actor
import scala.actors.Actor._


object Main {
  def main(args: Array[String]) {
    /* if (args.length < 2) {
       System.err.println("Usage: SparkGrep <host> <input_file>")
       System.exit(1)
     }*/
    val conf = new SparkConf().setAppName("Main").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(args(0), 2).cache()

    val flattenAllGeneFilter = inputFile.flatMap(line => line.split(" ").filter(line => line.contains("gene")))
    val quantifyFAGF = flattenAllGeneFilter.flatMap(word => word.split(" ")).map(x => (x, 1));
    //    Nk
    //TOTAL OCCURANCES OF EACH WORD
    val TotalOccurancesOfWord = quantifyFAGF.reduceByKey(_ + _) //(term, total # of terms in all documents)
    //DISTINCT LIST OF WORDS
    val wordList = inputFile.flatMap(line => line.split(" ").filter(line => line.contains("gene"))).distinct.collect()
    //   D
    //NUMBER OF DOCUMENTS AKA LINES
    val numDocuments = Source.fromFile(args(0)).getLines.size

    val addDocumentId = inputFile.zipWithIndex.map{ case(x,i) =>("doc_"+i,x) }
    val splitNquantifyNgenefilter = addDocumentId.flatMap( x =>  x._2.split(" ").map(y => ((x._1,y),1))).filter(j => j._1._2.contains("gene"))
    //COUNT OF OCCURANCE OF EVERY DISTINCT WORD IN EVERY DOCUMENT
    val OccuranceOfWordPerDoc = splitNquantifyNgenefilter.reduceByKey(_+_).collect()

    

    //TotalOccurancesOfWord.foreach(println)
    //OccuranceOfWordPerDoc.foreach(println)

    var numDocsWhereWord: Map[String,Int] =Map();
    var OccurWordPerDoc: Map[String, Array[Int]] =Map();

    for(singleword <- wordList ){
      //    Ni
      //CREATION OF ARRAY OF ARRAY (MATRIX) THAT HAS THE COUNT OF EVERY DISTINCT WORD IN EVERY DOCUMENT
      //INCLUDES ZEROS
      OccurWordPerDoc += (singleword -> iterateOccuranceWordPerDoc(OccuranceOfWordPerDoc, singleword, numDocuments))
      //   d : ti exist in d
      //CREATION OF COUNT OF DOCS THAT CONTAIN WORD
      numDocsWhereWord += (singleword -> iterateNumDocWord(OccuranceOfWordPerDoc,singleword))
    }
    println("num instanes per doc")
    for( word <- OccurWordPerDoc){
      print(word._1+"  ")
      word._2.foreach(print)
      println(" ")
    }

    println(" ")
    println("numDocs where Word ")
    numDocsWhereWord.foreach{print}
    println("\n ")
    println("total number of documents "+numDocuments)

    //for( word <- OccurWordPerDoc){

    //}
  }

  def iterateOccuranceWordPerDoc( list : Array[((String,String),Int)], wd:String,numD:Int):Array[Int] = {
    var singleWordArray = new Array[Int](numD)
    for(occur <- list) {
      val docnum = occur._1._1.split("_")
      if( occur._1._2 == wd) { singleWordArray(docnum(1).toInt) = occur._2; }
    }
    return singleWordArray
  }


  def iterateNumDocWord( list: Array[((String,String),Int)] ,wd:String ):Int = {
    var instanceofWord=0;
    for(occur <- list) {
      if (occur._1._2 == wd) { instanceofWord = instanceofWord + 1;}
    }
    return instanceofWord
  }

}