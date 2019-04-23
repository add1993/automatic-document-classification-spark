from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

sqlContext = SQLContext(sc)
newDF = [
    StructField("id", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("label", DoubleType(), True)]
finalSchema = StructType(fields=newDF)
dataset = sqlContext.read.format('csv').options(header='true',schema=finalSchema,delimiter='|').load('/FileStore/tables/dataset.csv')
#types = [f.dataType for f in dataset.schema.fields]
#print(types)
dataset = dataset.withColumn("label", dataset["label"].cast(DoubleType()))
dataset = dataset.withColumn("id", dataset["id"].cast(IntegerType()))
training, test = dataset.randomSplit([0.8, 0.2], seed=12345)
#types = [f.dataType for f in training.schema.fields]
#print(types)
#exit()
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=2, regParam=0.001)
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, nb])

# Fit the pipeline to training documents.
model = pipeline.fit(training)
result = model.transform(test)\
    .select("features", "label", "prediction")
correct = result.where(result["label"] == result["prediction"])
accuracy = correct.count()/test.count()
print("Accuracy of model = "+str(accuracy))
test_error = 1 - accuracy
print ("Test error = "+str(test_error))
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
metric = evaluator.evaluate(result)
print("F1 metric = "+ str(metric))

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
metric = evaluator.evaluate(result)
print("Recall = "+ str(metric))

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
metric = evaluator.evaluate(result)
print("Precision = "+ str(metric))
model.save("/FileStore/nbmodelNew")