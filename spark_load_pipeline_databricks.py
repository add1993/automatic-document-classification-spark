from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

model = PipelineModel.load('/FileStore/lrmodel')
newDF = [
    StructField("id", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("label", DoubleType(), True)]
finalSchema = StructType(fields=newDF)
dataset = sqlContext.read.format('csv').options(header='true',schema=finalSchema,delimiter='|').load('/FileStore/tables/dataset.csv')
dataset = dataset.withColumn("label", dataset["label"].cast(DoubleType()))
dataset = dataset.withColumn("id", dataset["id"].cast(IntegerType()))

result = model.transform(dataset)\
    .select("features", "label", "prediction")
correct = result.where(result["label"] == result["prediction"])
accuracy = correct.count()/dataset.count()
print("Accuracy of model = "+str(accuracy))