package com.mayank.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    /** Logger to log the messages and any errors*/
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());


    /** constructor*/
    public TwitterProducer() {}


    /** Main function just invoking the run() method of the TwitterProducer*/
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    /**@Params :void
     * @Returns :void
     *
     * Function does
     * 1.Get a Twitter client that is already connected to twitter public streams
     * 2.Get a Kafka Producer connected to a particular topic , throwing all the raw tweets
     * 3.While client is not done , we keep streaming the tweets into the topic (or shutdown the twitter producer)
     * */
    public void run()
    {
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        /** Get a twitter Client and pass the message Queue, that is storing the data*/
        Client client = createTwitterClient(msgQueue);

        //connect to twitter client
        client.connect();

        // create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application....");
            logger.info("Shutting down client from twitter....");
            client.stop();
            logger.info("closing producer....");
            producer.close();
            logger.info("done!..");
        }));


        //loop to send tweets to kafka on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null)
            {
                logger.info(msg);
                producer.send(new ProducerRecord<>("16_04_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null)
                        {
                            logger.error("Something bad happened ",e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }



    /**
     * @param msgQueue
     * @return TwitterClient
     *
     */
    public Client createTwitterClient(BlockingQueue<String> msgQueue)
    {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        /** List of all the related terms for which tweets we are searching*/
        List<String> terms = Lists.newArrayList("covid-19","virus","corona","COVID-19","VIRUS","CORONA");

        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        /** Access keys and Token defined , the user of this code must use their ouwn personal keys*/
        String consumerKey = "0hyhuGwO1P1iWN1ju0fMsuehO";
        String consumerSecret = "Zfb2t9rvtCOKVQgmilTbJVKJSmy3tCrckWIu7oPeFHa31OS3bF";
        String token = "847791952103788545-eIdw3gQW1WN4SveaDfRiM3MD2mUT6PV";
        String secret  = "YhlrTL1v9ATImGDsDqdqQ9VDBNPx6azKZHGDBnJ7uveyT";


        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret,token,secret);


        //Creating a client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    /**
     * @return KafkaProducer
     */
    public KafkaProducer<String,String> createKafkaProducer()
    {

        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

//        //high throughput producer (at eh expense ofa bit of latency and CPU cycle)
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
