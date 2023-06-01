package com.hobday

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object TwitterStreamingAnalytics {
  
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials */
  def setupTwitter(): Unit = {
    import scala.io.Source

    val lines = Source.fromFile("data/twitter.txt")
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
    lines.close()
  }

  def main(args: Array[String]) {
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "TwitterStreamingAnalytics", Seconds(1))
    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText)
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( 
      (x,y) => x + y, 
      (x,y) => x - y, 
      Seconds(300), 
      Seconds(1)
    )
    //alternative: val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    sortedResults.print
    
    ssc.checkpoint("C:/cpt/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
