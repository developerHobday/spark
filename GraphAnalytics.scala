package com.hobday

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.graphx._

/** Analytics with GraphX */
object GraphAnalytics {
  
  // Extract node ID -> name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(VertexId, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      val id:Long = fields(0).trim().toLong
      if (id < 6487) {  // ID's above 6486 not valid
        return Some( fields(0).trim().toLong, fields(1))
      }
    } 
  
    None // flatmap will just discard None results, and extract data from Some results.
  }
  
  /** Transform an input line from graph.txt into a List of Edges */
  def makeEdges(line: String) : List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    val origin = fields(0)
    for (x <- 1 until (fields.length - 1)) {
      // Attribute field is unused, but in other graphs could
      // be used to deep track of physical distances etc.
      edges += Edge(origin.toLong, fields(x).toLong, 0)
    }
    
    edges.toList
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "GraphAnalytics")

    val names = sc.textFile("data/names.txt")
    val verts = names.flatMap(parseNames)
    val lines = sc.textFile("data/graph.txt")
    val edges = lines.flatMap(makeEdges)    
    
    val default = "Nobody"
    val graph = Graph(verts, edges, default).cache()
    
    println("\nTop 10 most-connected nodes:")
    graph.degrees.join(verts)
      .sortBy(_._2._1, ascending=false)
      .take(10)
      .foreach(println)


    println("\nComputing path length from start to end")
    val root: VertexId = 5306    
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)( 
        // vertex program
        (id, attr, msg) => math.min(attr, msg),
        
        // send message function 
        triplet => { 
          if (triplet.srcAttr != Double.PositiveInfinity) { 
            Iterator((triplet.dstId, triplet.srcAttr+1)) 
          } else { 
            Iterator.empty 
          } 
        }, 
        (a,b) => math.min(a,b) ).cache()
    
    // Print out the first 100 results:
    bfs.vertices.join(verts).take(100).foreach(println)
    
    println("\n\nBFS to end")      
    bfs.vertices.filter(x => x._1 == 14).collect.foreach(println)
    
  }
}