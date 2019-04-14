import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from kafka import KafkaConsumer

brokers, topic = sys.argv[1:]
print("Starting streaming program")
sc = SparkContext("local[2]", "StreamData")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 1)
file = open("streamlog.txt", "a")
model = PipelineModel.load('C:\\Spark\\spark-2.3.3-bin-hadoop2.7\\bin\\model')
file.write("Model loaded\n")
consumer = KafkaConsumer(topic, bootstrap_servers = ['localhost:9092'])

for text in consumer:
    data = str(text.value, 'utf-8')
    iList = data.split("|")
    dataset = sc.parallelize([{'id':iList[0], 'text':iList[1], 'label':iList[2]}]).toDF()
    dataset = dataset.withColumn("label", dataset["label"].cast(DoubleType()))
    dataset = dataset.withColumn("id", dataset["id"].cast(IntegerType()))
    result = model.transform(dataset).select("features", "label", "prediction")
    result.show()
    file.write("Data received"+"\n")
    file.write(data+"\n")
    file.write("Prediction  |  Label\n")
    for row in result.rdd.collect():
        file.write(str(row.asDict()['prediction']) + "  | " + str(row.asDict()['label']) + "\n")

ssc.start()
ssc.awaitTermination()