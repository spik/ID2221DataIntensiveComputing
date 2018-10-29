package consumer

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra
      val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
      val session = cluster.connect()
      //make keyspace and table
      session.execute("CREATE KEYSPACE IF NOT EXISTS twitter_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
      session.execute("CREATE TABLE IF NOT EXISTS twitter_space.twitter (hashtag text PRIMARY KEY, count int)")

      val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HashtagCount")
      val streamingContext = new StreamingContext(sparkConf, Seconds(1))
      streamingContext.checkpoint("checkpoint")

      var count = 0

      // make a connection to Kafka and read (key, value) pairs from it
      val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")

      //receive data (key,value) pairs from Kafka with receiver less approach
      //Spark periodically queries Kafka for the latest offsets i neach topic and partition
      //Kafka consumer API is used to read the defined ranges of offsets from Kafka
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        streamingContext,
        kafkaConf,
        Set("twitter")
      )

      val pairs = messages.map(x => (x._1, x._2))

      // State is the result from the last iteration. Therefore, we need a sum a count from the prevous iteration so that we can count the numer of iterations.
      def mappingFunc(key: String, value: Option[String], state: State[Int]): (String, Int) = {

        //If the state exist, we have a count from a prevous iteration. Increment the current iteration
        if(state.exists){

          //Get the state, which is the previous count
          val prevCount = state.get

          //increment count
          count = prevCount + 1
        }
        //In this case, it is the first iteration, so we don't have a count yet. Set the iteraction to 1 as it is the first iteration
        else{
          count = 1
        }

        //Update the state for the next execution
        state.update(count)

        //Return the key and the count
        return (key, count)
      }

      //create statespec for setting specifications of the mapWithState operation
      val stateSpec = StateSpec.function(mappingFunc _)
      val stateDstream = pairs.mapWithState(stateSpec)

      //store the result in Cassandra
      stateDstream.foreachRDD(rdd => rdd.saveToCassandra("twitter_space", "twitter", SomeColumns("hashtag", "count")))

      streamingContext.start()
      streamingContext.awaitTermination()
      session.close()
  }
}
