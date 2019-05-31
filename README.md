# Automatic Document Classification 
This folder contains the code to run classification on streaming data.
We need to follow the given steps:

#### Step 1 : Data Extraction from Guardian APIs
Firsly, we need to generate data locally to train our model. We can use the local_data_collection.py and specify the dates and the key. This code will create the specified dataset(headers : id,text,label) and fix the category and labels file. The code will automatically increment the dates and keep on collecting the data specified by the iterations.

To use this API, you will need to sign up for API key here:<br/>
https://open-platform.theguardian.com/access/

There are multiple categories such as Australia news, US news, Football, World news, Sport, Television & radio, Environment, Science, Media, News, Opinion, Politics, Business, UK news, Society, Life and style, Inequality, Art and design, Books, Stage, Film, Music, Global, Food, Culture, Community, Money, Technology, Travel, From the Observer, Fashion, Crosswords, Law.

#### Step 2 : Create a model using the extracted data
After this I uploaded the dataset on databricks and create the pipeline model using the spark_pipeline_databricks.py. This file will save the pipeline in a folder and will show the accuracy of the model by using dataset for 80% training and 20% testing.

#### Step 3 : Run stream_producer.py to use the extracted model to classify streaming data
Now we will using the stream_producer.py to create a kafka stream data and use the spark_stream.py file to read the stream and load the created model and generate the prediction on the fly. Please, add the saved model full path in the spark_stream.py file.

#### Commands
start zookeeper : bin/zookeeper-server-start.sh config/zookeeper.properties <br/><br/>

start kafka server : bin/kafka-server-start.sh config/server.properties<br/><br/>

produce data : ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test<br/><br/>

consume data : ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning<br/><br/>

submit code : ./spark-submit.cmd  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 sparkstream.py localhost:9092 test<br/><br/>


For solving memory full errors : <br/><br/>

Since you are running Spark in local mode, setting spark.executor.memory won't have any effect, as you have noticed. The reason for this is that the Worker "lives" within the driver JVM process that you start when you start spark-shell and the default memory used for that is 512M. You can increase that by setting spark.driver.memory to something higher, for example 5g. You can do that by either:<br/><br/>

    setting it in the properties file (default is spark-defaults.conf),

    spark.driver.memory              5g

    or by supplying configuration setting at runtime

    $ ./bin/spark-shell --driver-memory 5g

Note that this cannot be achieved by setting it in the application, because it is already too late by then, the process has already started with some amount of memory.
