/* Gabriela Voll Gracie Liang */

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


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
    val quantifyFAGF = flattenAllGeneFilter.flatMap(word => word.split(" ")).map(x => (x, 1));
    //    Nk
    //TOTAL OCCURANCES OF EACH WORD
    val TotalOccurancesOfWord = quantifyFAGF.reduceByKey(_ + _) //(term, total # of terms in all documents)
    //DISTINCT LIST OF WORDS
    val wordListBad = inputFile.flatMap(line => line.split(" ").filter(line => line.contains("gene_"))).distinct
    val wordList = inputFile.flatMap(line => line.split(" ").filter(line => line.contains("gene_"))).distinct.collect()

    //   D
    //NUMBER OF DOCUMENTS AKA LINES
    val numDocuments = inputFile.collect().length

    val addDocumentId = inputFile.zipWithIndex.map{ case(x,i) =>("doc_"+i,x) }
    val splitNquantifyNgenefilter = addDocumentId.flatMap( x =>  x._2.split(" ").map(y => ((x._1,y),1))).filter(j => j._1._2.contains("gene_"))

    //COUNT OF OCCURANCE OF EVERY DISTINCT WORD IN EVERY DOCUMENT
    val OccuranceOfWordPerDoc = splitNquantifyNgenefilter.reduceByKey(_+_).collect()

    val rddToFindDocWithWordX = splitNquantifyNgenefilter.reduceByKey(_+_)

    //   d : ti exist in d
    //CREATION OF COUNT OF DOCS THAT CONTAIN WORD
    val docWithWordX = rddToFindDocWithWordX.map(y => (y._1._2, 1)).reduceByKey(_+_)

    //finding the idf for each gene term
    val idf = docWithWordX.map(x => (x._1,math.log ( numDocuments / x._2)))

    var OccurWordPerDoc: Map[String, Array[Double]] =Map();

    //var Occur = wordListBad.map( x => (x, iterateOccuranceWordPerDoc(OccuranceOfWordPerDoc, x, numDocuments)))
    for(singleword <- wordList ){
      //    Ni
      //CREATION OF ARRAY OF ARRAY (MATRIX) THAT HAS THE COUNT OF EVERY DISTINCT WORD IN EVERY DOCUMENT
      //INCLUDES ZEROS
      OccurWordPerDoc += (singleword -> iterateOccuranceWordPerDoc(OccuranceOfWordPerDoc, singleword, numDocuments))
    }

    var tf = OccurWordPerDoc
    //MAKE ME ACTOR THIS ONE ES
    for( wordarray <- tf){
      for(  i <- 0 until wordarray._2.length ){
        wordarray._2(i)= (wordarray._2(i) / (TotalOccurancesOfWord.lookup(wordarray._1).mkString).toDouble) * ((idf.lookup(wordarray._1).mkString).toDouble)
      }
    }
    println("\n\n\t IDF of ALL WORDS in ALL DOCUMENTS ")

    for( word <- tf){
      print(word._1+"  ")
      for( x <- word._2){ print("   "+x+"   ")}
      println(" ")
    }

    val WordMatrixLength = tf.map( x => (x._1, x._2.map(y => (y*y)).reduce((a, b) => a + b))).map(x => (x._1 , Math.sqrt(x._2)))

    var SemanticSimilarity: Map[(String,String),Double] = Map()

    //MAKE ME ACTOR
    //CALUCULATION OF SEMANTIC SIMILARITY
    var skip = 1
    for(word <- tf ){
      var matchskip = 0
      for(word2 <- tf){
        if(matchskip >= skip ) {
          var top = 0.00
          //CAlCULATION OF TOP DOT PRODCUT OF TWO VECTORS
          for( i <- 0 until word._2.length){ top = top + (word._2(i)*word2._2(i))}
          //CALCUSTION OF BOTTOM MULITPLICTION OF VECTOR LENGTH
          var bot = (WordMatrixLength.get(word._1).mkString).toDouble * (WordMatrixLength.get(word2._1).mkString).toDouble
          SemanticSimilarity += ( (word._1, word2._1) -> (top/bot) )
        }
        matchskip = matchskip+1
      }
      skip= skip+1
    }
    println("Semantic Similarity")
    SemanticSimilarity.foreach(println)

  }


  //MAKE ME ACTOR
  def iterateOccuranceWordPerDoc( list : Array[((String,String),Int)], wd:String,numD:Int):Array[Double] = {
    var singleWordArray = new Array[Double](numD)
    for(occur <- list) {
      val docnum = occur._1._1.split("_")
      if( occur._1._2 == wd) { singleWordArray(docnum(1).toInt) = occur._2; }
    }
    return singleWordArray
  }


}