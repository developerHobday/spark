package com.hobday

import org.apache.spark._
import org.apache.log4j._

/** Find the node with the most links. */
object Graph {
  
  // Function to extract the node and number of connections from each line
  def countConnections(line: String): (Int, Int) = {
    val elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
 
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")    
    val lines = sc.textFile("data/graph.txt")
    
    val pairings = lines.map(countConnections)
    val connectionsByNode = pairings.reduceByKey( (x,y) => x + y )
    val flipped = connectionsByNode.map( x => (x._2, x._1) )
    val mostConnected = flipped.max()

    println(mostConnected) 
  }
  
}
