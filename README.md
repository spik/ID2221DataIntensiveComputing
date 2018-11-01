# ID2221DataIntensiveComputing

This application was done as the project assignment in the course "Data Intensive Computing" (ID2221) at KTH (Royal Institute of Technology) in October 2018. 
The assignmnet was done together with a group partner. 

The application is a producer-consumer application in Apache Kafka. The producer gets tweets related to a specific hashtag from the 
Twitter API and sends the hashtag as a key and the tweet as a value to the consumer. The consumer counts the number of tweets related 
to each hashtag and stores the result in a table in Apache Cassandra. The consumer also calculates the total number of tweets. 
Here is an example output in Cassandra:

|   hashtag   |   count   |
|-------------|-----------|
|#ApacheKafka |     14    |
|#RabbitMQ    |     12    |
|Total        |     26    |
