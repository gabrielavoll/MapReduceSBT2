/* Gabriela Voll Gracie Liang */

import javax.annotation.Resource

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.actors.Actor._
import scala.actors.Actor
import java.io._
import scala.io._


object Main {
  def main(args: Array[String]) {
    /* if (args.length < 2) {
       System.err.println("Usage: SparkGrep <host> <input_file>")
       System.exit(1)
     }*/
    val conf = new SparkConf().setAppName("Main").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(args(0), 2).cache()

    val flattenAllGeneFilter = inputFile.flatMap(line => line.split(" ").filter(line => line.contains("gene_")))
    val quantifyFAGF = flattenAllGeneFilter.flatMap(word => word.split(" ")).map(x => (x, 1))
    //    Nk   TOTAL OCCURANCES OF EACH WORD
    val TotalOccurancesOfWord = quantifyFAGF.reduceByKey(_ + _).collect().toMap //(term, total # of terms in all documents)
    //DISTINCT LIST OF WORDS
    val wordList = inputFile.flatMap(line => line.split(" ").filter(line => line.contains("gene_"))).distinct

    //    D     NUMBER OF DOCUMENTS AKA LINES
    val numDocuments = inputFile.collect().length

    val addDocumentId = inputFile.zipWithIndex.map{ case(x,i) =>("doc_"+i,x) }
    val splitNquantifyNgenefilter = addDocumentId.flatMap( x =>  x._2.split(" ").map(y => ((x._1,y),1))).filter(j => j._1._2.contains("gene_"))

    //COUNT OF OCCURANCE OF EVERY DISTINCT WORD IN EVERY DOCUMENT
    val OccuranceOfWordPerDoc = splitNquantifyNgenefilter.reduceByKey(_+_).collect()
    val rddToFindDocWithWordX = splitNquantifyNgenefilter.reduceByKey(_+_)

    //    d : ti exist in d       CREATION OF COUNT OF DOCS THAT CONTAIN WORD
    val docWithWordX = rddToFindDocWithWordX.map(y => (y._1._2, 1)).reduceByKey(_+_)

    //finding the idf for each gene term
    val idf = docWithWordX.map(x => (x._1,math.log10(( numDocuments.toDouble / x._2.toDouble)))).collect().toMap
    //ni
    val OccurWordPerDoc = wordList.map( x => (x, iterateOccuranceWordPerDoc(OccuranceOfWordPerDoc, x, numDocuments)))

    val tf = OccurWordPerDoc.map(x => ( x._1, tfcreation( x._2, TotalOccurancesOfWord.get(x._1).mkString.toDouble)))

   //al tdidfFile = new PrintWriter("TDIDFoutput.txt")

    var tfidf = tf.map(x => (x._1, x._2.map( y => y*(idf.get(x._1).mkString).toDouble)))

    println("\t TDIDF of ALL WORDS in ALL DOCUMENTS \n\n")
    for(word <- tfidf){
      print(word._1+"  ")
      for( x <- word._2){ print("   "+x+"   ")}
      print("\n")
    }

    //Matrix Length of all Words IE the lower part of Semantic Similarity
    val WordMLen = tfidf.map( x => (x._1, x._2.map(y => (y*y)).reduce((a, b) => a + b))).map(x => (x._1 , Math.sqrt(x._2))).collect().toMap

    //CALUCULATION OF SEMANTIC SIMILARITY
    //val semanticSimActor = actor {
    var SemanticSim2 = tfidf.cartesian(tfidf)
    var SemanticSim1 = SemanticSim2.map( x => ((x._1._1, x._2._1),List(x._1._2.toList,x._2._2.toList) transpose))
    var SemanticSim0 = SemanticSim1.map( x => ((x._1._1, x._1._2), (x._2.map(x => x.reduce(_*_))).reduce(_+_)))
    val SemanticSim = SemanticSim0.map( x => ( (x._1._1,x._1._2), (x._2/((WordMLen.get(x._1._1).mkString).toDouble * (WordMLen.get(x._1._2).mkString).toDouble) ) )).filter(x => x._2 != 0.0)

    println("\tSemantic Similarity\n\n")
    for( word <- SemanticSim){
      print(word._1+"  : "+word._2)
      print("\n")
    }

    println("\n\nPROGRAM DONE CHECK THE OUTPUT FILES\n\n")
  }

  def iterateOccuranceWordPerDoc( list : Array[((String,String),Int)], wd:String,numD:Int):Array[Double] = {
    var singleWordArray = new Array[Double](numD)
    for(occur <- list) {
      val docnum = occur._1._1.split("_")
      if( occur._1._2 == wd) { singleWordArray(docnum(1).toInt) = occur._2; }
    }
    return singleWordArray
  }

  def tfcreation( numlist:Array[Double], z: Double):Array[Double] = {
    var newnumlist= numlist.map(x => x/z)
    return newnumlist
  }
}