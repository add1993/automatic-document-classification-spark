# Automatic Document Classification 
This folder contains the code to run classification on streaming data.
We need to follow the given steps:

#### Step 1 : Data Extraction from Guardian APIs
Firsly, we need to generate data locally to train our model. We can use the local_data_collection.py and specify the dates and the key. This code will create the specified dataset(headers : id,text,label) and fix the category and labels file. The code will automatically increment the dates and keep on collecting the data specified by the iterations.

#### Step 2 : Create a model using the extracted data
After this I uploaded the dataset on databricks and create the pipeline model using the spark_pipeline_databricks.py. This file will save the pipeline in a folder and will show the accuracy of the model by using dataset for 80% training and 20% testing.

#### Step 3 : Run stream_producer.py to use the extracted model to classify streaming data
Now we will using the stream_producer.py to create a kafka stream data and use the spark_stream.py file to read the stream and load the created model and generate the prediction on the fly. Please, add the saved model full path in the spark_stream.py file.
