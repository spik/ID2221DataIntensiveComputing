package producer

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage

import java.util._
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer

import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

class Hashtag() {
   var hashtag = ""
   var prevCount = 0
   var prevList = ArrayBuffer[twitter4j.Status]()
}

object Producer extends App {

    val broker = "localhost:9092"

    // create instance for properties to access producer configs
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    //send event to consumer
    def sendEvent(key: String, message: String) = {

        //ProducerRecord − The producer manages a buffer of records waiting to be sent.
        val data = new ProducerRecord[String, String]("twitter", key, message)

        producer.send(data)
    }

    //Checks if a tweet is new since the last time tweets were requested for the hashtag
    def isNewTweet(prevList: ArrayBuffer[twitter4j.Status], status: twitter4j.Status): Boolean = {

      //If the list is empty, we have no previous tweets for the hashtag so the tweet is new
      if(prevList.isEmpty){
        return true
      }

      for(prevTweet <- prevList){

        //The tweet is already in the list of previous tweets for the hashtag, so this is not a new tweet
        //We know this as each tweet has a unique ID, so if the tweet we check has the same ID as a tweet in the list we know it's the same tweet
        if(prevTweet.getId() == status.getId()){
          return false
        }
      }

      //Did not find the tweet in the list of previous tweets, so it is a new tweet
      return true
    }

    //For debugging
    def printArray(prevList: ArrayBuffer[twitter4j.Status]) ={
      for(prevTweet <- prevList){
        println("Test tweet: " + prevTweet.getText())
      }
    }

    val twitterConsumerKey=""
    val twitterConsumerSecret=""
    val twitterOauthAccessToken=""
    val twitterOauthTokenSecret=""

    //A builder that can be used to construct a twitter4j configuration with desired settings
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
    .setOAuthConsumerKey(twitterConsumerKey)
    .setOAuthConsumerSecret(twitterConsumerSecret)
    .setOAuthAccessToken(twitterOauthAccessToken)
    .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

    val twitterFactory = new TwitterFactory(cb.build())
    val twitter = twitterFactory.getInstance()

    var hashtag1 = new Hashtag()
    hashtag1.hashtag = " #RabbitMQ "

    var hashtag2 = new Hashtag()
    hashtag2.hashtag = " #ApacheKafka "

    val hashtags = Array(hashtag1, hashtag2)

    while (true) {

      //Go through the hashtags, as we want to periodically check each hashtag for new tweets related to them
      for(hashtag <- hashtags){
        println("Hashtag " + hashtag.hashtag)

        val query = new Query(hashtag.hashtag)
        query.setCount(100)

        val format = new SimpleDateFormat("y-M-d")
        val todaysDate = format.format(Calendar.getInstance().getTime())
        println("Get tweets since " + todaysDate)
        query.setSince(todaysDate)
        query.lang("en")

        //Send a query request to Twitter API
        val result = twitter.search(query)

        //Get the tweets from the result
        val statuses = result.getTweets()
        var lowestStatusId = Long.MaxValue
        println("Number of tweets: " + statuses.size)

        //If no new tweets for this hashtag have been made since the last check, no need to send any new events to the server
        if(hashtag.prevCount < statuses.size){

          //Go through all the tweets in the result from the query
          for (status <- statuses.asScala){

            //Only send new tweets to the consumer
            if(isNewTweet(hashtag.prevList, status)){

              println("New tweet discovered, send")

              //Update the previous count so that next iteration we can check if any new tweets have been made
              hashtag.prevCount = statuses.size

              //Send hashtag to consumer
              sendEvent(hashtag.hashtag, status.getText())

              //As this is sent for every hashtag, we will get the total count for all hashtags combined
              //We can have any value as we don't use the value anyway
              sendEvent("Total count ", "total")
              println("DONE sending event")

              //Add the new tweet to the list of previous tweets for this hashtag, so that we know next time that we don't have to send this tweet again
              hashtag.prevList += status

              lowestStatusId = Math.min(status.getId(), lowestStatusId)
            }
          }
          query.setMaxId(lowestStatusId - 1)
        }

        //Check for updates (new tweets) every 10 seconds
        Thread.sleep(10000)
      }
    }

    if(producer != null){
      producer.close()
    }
}
