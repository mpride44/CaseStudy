# Project Requirements
 1. Code Editor (Java,Scala,Python)  : IntellIj preferred
 2. Kafka 2.13 2.4.1 => Scala version is 2.13 and kafka version is 2.4.1
 3. Spark 2.4.5
 4. MongoDB @4.2 community version
 5. Flask REST API
 6. Postman tool
 
# Useful Commands during project
1.to start mongoDb process in background: 
	mongod --config /usr/local/etc/mongod.conf --fork,
	brew services start mongodb-community@4.2,
	brew services stop monogdb-community@4.2
      
2.to start zookeeper:
	zookeeper-server-start config/zookeeper.properties

3.to start kafka:
	kafka-server-start config/server.properties

4.to create a kafka-topic:
	kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic topic-name

5.list all the topics:
	kafka-topics.sh --list --zookeeper localhost:2181

6.creating a kafka-console-producer: 
	kafka-console-producer --bootstrap-server localhost:9092 --topic topic-name

7.creating and consuming messages: 
	kafka-console-consumer --bootstrap-server localhost:9092 --topic topic-name --from-beginning
      
8.delete a consumer group: 
	kafka-consumer-groups --bootstrap-server localhost:9092 -delete --group group-name

9.delete a topic: 
	kafka-topics --zookeeper localhost:2181 --delete --topic topic-name

10.describe all groups: 
	kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe

11.to import data into mongoDB: 
	mongoimport -d databasename -c collectionname --file filename --jsonArray

# Project Description
This project is all about collect data from twitter related to corona virus based on some keywords and then answer some        queries. This project is completed in two phases. First phase is to collect data from twitter and then after transformation store that data in database. Second phase is to write an API service which can be queried to get the data from database. 

# Phase 1: Collection of data from Twitter and Storing data in database.
We use __Kafka__ to get the data from twitter and then use __Spark Streaming__ to get the data from Kafka topic and __MongoDB__ to store the data. First we need to create the developer account on twitter and generate keys. Then
1. Run zookeeper and kafka .
2. Make a topic in kafka with arbitrary  partitions and replication factor (recommended 3 and 1 respectively).
3. Open a code editor, load Twitter Producer code , and change all the pom.xml file as given TwitterProducer pom.xml              file. The Java version is 8.
4. Change the topic as your topic name.
5. Run the twitterProducer main()      
6. Open  SparkKafkaConnect program  as Scala program, configure the build.sbt file(in sbt)/(maven in gradle).
7. Subscribe the topic and select the mongodb(cloud or local) Database and collection.
8. Run the SparkKafkaConnect program.
9. All your filtered data is stored in mongoDB database.
      
# Phase 2: Writing an API service
We build the Flask REST API which can be queried using Postman.

# Project Directory Structure For REST API
      CaseStudy 
            Project 
                __init__.py
            Test
                __init__.py
                test_error.py
            run.py
            
# Initial Installation
      
## To start the application
1. Make a directory named "Case Study" by command -> mkdir CaseStudy
2. Move to that directory -> cd CaseStudy
      Before executing the third command make sure that you have install higher version of Python 3 and pip. 
3. Install the python virtual environment -> pip install pipenv
4. Install Flask -> pipenv install Flask 
5. To start the virtual environment -> pipenv shell
6. To run the application -> python run.py

## To test the application
1. Install pytest -> pip install pytest
2. Run test command -> pytest -v

# Explanation
Main directory name is __Case Study__ under this three files are present Project, run.py and Test.

Project -> It is a package and contains the code for the API.

run.py -> Python file to run the application.

Test -> It is a package and contains the test case for testing the API. __"test_error.py"__ contains the all the code for testing the API.

## Step 1
Run the __"run.py"__. It will run the application created in "__init__.py" in project package. In "__init__.py", Firsty we connect to the database with the help of pymongo driver, it created an instance of __MongoClient__ and then read the data from the database.

A root-end point for API request is also created in this. Once the request is received from the API, it executed its corresponding function and return the response according to the corresponding request.

## Step 2
It contains all the test cases to test the API. Run the command __"pytest -v"__ to see the result.
