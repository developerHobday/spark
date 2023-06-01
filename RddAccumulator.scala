package com.hobday

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

/** Finds the degrees of separation between two characters in a graph
 */
object RddAccumulator {
  
  val startCharacterID = 5306 //SpiderMan
  val targetCharacterID = 14 //ADAM 3,031 (who?)
  
  // We make our accumulator a "global" Option so we can reference it in a mapper later.
  var hitCounter:Option[LongAccumulator] = None
  
  type BFSData = (Array[Int], Int, String)
  type BFSNode = (Int, BFSData)
    
  /** Converts a line of raw input into a BFSNode */
  def convertToBFS(line: String): BFSNode = {
    val fields = line.split("\\s+")
    val heroID = fields(0).toInt
    
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for ( connection <- 1 until (fields.length - 1)) {
      connections += fields(connection).toInt
    }
    
    var color:String = "WHITE"
    var distance:Int = 9999
    
    if (heroID == startCharacterID) {
      color = "GRAY"
      distance = 0
    }
    
    (heroID, (connections.toArray, distance, color))
  }
  
  def createStartingRdd(sc:SparkContext): RDD[BFSNode] = {
    val inputFile = sc.textFile("data/marvel-graph.txt")
    inputFile.map(convertToBFS)
  }
  
  /** Expands a BFSNode into this node and its children */
  def bfsMap(node:BFSNode): Array[BFSNode] = {
    
    val characterID:Int = node._1
    val data:BFSData = node._2
    
    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var color:String = data._3
    
    var results:ArrayBuffer[BFSNode] = ArrayBuffer()
    
    if (color == "GRAY") { 
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"
        
        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }
        
        val newEntry:BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }      
      color = "BLACK"
    }
    
    val thisEntry:BFSNode = (characterID, (connections, distance, color))
    results += thisEntry    
    results.toArray
  }
  
  /** Combine nodes for the same heroID, preserving the shortest length and darkest color. */
  def bfsReduce(data1:BFSData, data2:BFSData): BFSData = {
    
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3
    
    // Default node values
    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()
    
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }
    
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }
    
    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }
	if (color1 == "GRAY" && color2 == "GRAY") {
	  color = color1
	}
	if (color1 == "BLACK" && color2 == "BLACK") {
	  color = color1
	}
    
    (edges.toArray, distance, color)
  }
    
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "DegreesOfSeparation") 
    
    hitCounter = Some(sc.longAccumulator("Hit Counter"))    
    var iterationRdd = createStartingRdd(sc)

    for (iteration <- 1 to 10) {
      println("BFS Iteration# " + iteration)
   
      // explore connected nodes and mark as gray
      val mapped = iterationRdd.flatMap(bfsMap)
      
      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // updates the accumulator.  
      println("Processing " + mapped.count() + " values.")
      
      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount + 
              " different direction(s).")
          return
        }
      }
         
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }
}