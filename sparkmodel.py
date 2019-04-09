from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)
newDF = [
    StructField("id", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("label", DoubleType(), True)]
finalSchema = StructType(fields=newDF)

# Prepare training documents from a list of (id, text, label) tuples.
dataset = sqlContext.read.format('csv').options(header='true',schema=finalSchema,delimiter='|').load('dataset.csv')
dataset = dataset.withColumn("label", dataset["label"].cast(DoubleType()))
dataset = dataset.withColumn("id", dataset["id"].cast(IntegerType()))
training, test = dataset.randomSplit([0.8, 0.2], seed=12345)

#training = sqlContext.createDataFrame([
#    (0, "a b c d e sc", 1.0),
#    (1, "b d", 0.0),
#    (2, "sc f g h", 1.0),
#    (3, "hadoop mapreduce", 0.0)
#], ["id", "text", "label"])

# Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)

# Prepare test documents, which are unlabeled (id, text) tuples.
#training = sqlContext.read.format('csv').options(header='false',delimiter='|',schema=schema).load('trainingdata.csv')
#test = sqlContext.createDataFrame([
#    (4, "sc i j k"),
#    (5, "l m n"),
#    (6, "sc hadoop sc"),
#    (7, "apache hadoop")
#], ["id", "text"])

# Make predictions on test documents and print columns of interest.
model.transform(test)\
    .select("features", "label", "prediction")\
    .show()

#prediction = model.transform(test)
#selected = prediction.select("id", "text", "probability", "prediction")
#for row in selected.collect():
#    rid, text, prob, prediction = row
#    print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))
