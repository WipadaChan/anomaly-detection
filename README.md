# Real-time anomaly detection with Azure Anomaly Detector API with Spark Structure Streaming 
This repo shows how you can use Anomaly Detector API with Streaming data feed in Azure Databricks. 
The Anomaly Detector API enables you to monitor and find abnormalities in your time series data by automatically identifying and applying the correct statistical models, regardless of industry, scenario, or data volume. Using your time series data, the API can find anomalies as a batch throughout your data, or determine if your latest data point is an anomaly.

## Overview Architecture 

![alt text](https://github.com/WipadaChan/anomaly-detection/blob/main/image/overview.png "Overview Architecture") 

## Prerequisites

1. You must have an [Anomaly Detector API resource](https://aka.ms/adnew). Before continuing, you will need the API key and the endpoint from your Azure dashboard.
   ![Get Anomaly Detector access keys](https://github.com/Azure-Samples/AnomalyDetector/blob/master/media/cognitive-services-get-access-keys.png "Get Anomaly Detector access keys")
2. Azure Databrick cluster (up and running). To create Azure Databrick cluster, plese refer to https://docs.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal You also need to install Azure Eventhubs library from Marven Central: **azure-eventhubs-spark** 
3. IoT Hub endpoint for feeding [Raspberry Pi Simulation data](https://azure-samples.github.io/raspberry-pi-web-simulator/). Follow instruction and hit start, so it will sending telemetry data to IoT Hub
4. Power BI Stream Dataset, for how to create please refer to [the link here](https://docs.microsoft.com/en-us/power-bi/connect-data/service-real-time-streaming#pushing-data-to-datasets). Please ensure that you create dataset with have same structure and data type as data that you want to push during writeStream step. 
Here is my Power BI Streaming Dataset structure: 
![alt text](https://github.com/WipadaChan/anomaly-detection/blob/main/image/PBISteamDataset.png)


### Anomaly Dector API
For real-time use case we can use "Last Point Detector API" where it requires at least 12 data points. In this case we need to wait and check each Spark Structure Streaming microbatch to have at least 12 data point. I leverage sample notebook for Last point detection API as [link here](https://github.com/Azure-Samples/AnomalyDetector/blob/master/ipython-notebook/API%20Sample/Latest%20point%20detection%20with%20the%20Anomaly%20Detector%20API.ipynb)

## Step:
I will walk you through step in my notebook. Before start ensure you have both Anomaly detection endpoint and key, and connection string to IoT Hub.

### Step 1. Define connection string to IoT Hub
Getting IoT Hub built-in enpoint from Azure Portal as ![below](https://github.com/WipadaChan/anomaly-detection/blob/main/image/getiotendpoint.png "Get IoT Endpoint")

```python
# Source with default settings
connectionString = "Endpoint=sb://xxxx.servicebus.windows.net/;SharedAccessKeyName=xxxx;SharedAccessKey=xxx=;EntityPath=xxxx"
ehConf = {
  'eventhubs.connectionString' : connectionString
}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
```

### Step 2. Read Stream from IoT Hub Endpoint
Define dataframe by using Spark Structure Streaming to readStream from IoT Endpoint

```python
df = spark \
  .readStream \
  .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider") \
  .options(**ehConf) \
  .load()
 
df = df.withColumn("body", df["body"].cast("string"))
```

Extract information from IoT messages, I need to extract messageId, temperature and humidity out of the Body element :

```python
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
 
 
schema = ( StructType()
  .add('messageId', StringType()) 
  .add('temperature', DoubleType())
  .add('humidity', DoubleType())     
)
 
    
df2 = df.select((df.enqueuedTime).alias("Enqueued_Time"),
                (df.systemProperties["iothub-connection-device-id"]).alias("Device_ID")
                ,(from_json(df.body.cast("string"), schema).alias("telemetry_json"))).select("Enqueued_Time","Device_ID", "telemetry_json.*")

df2.createOrReplaceTempView("device_telemetry_data")
```

For Anomaly Detector API to work it requires 2 columns: timestamp and value columns where timestamp need to be in this format YYYY-MM-DDTHH:mm:ssZ e.g. **2021-11-18T06:28:00Z** 

```python
#date_trunc('SECOND',Enqueued_Time) as timestamp
temperature = spark.sql( """ Select Enqueued_Time, 
concat(date_format(Enqueued_Time,"yyyy-MM-dd"),"T",date_format(date_trunc('MINUTE',Enqueued_Time), 'HH:mm:ssX')) as timestamp,
temperature as value  
from device_telemetry_data """)
```

Since the lowest ganularity that can do anomaly detection is a minute, so I need to do window aggregation to get max temperature in a minutes and open window for 15 minutes (in order to have enough data point for the API)
```python
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

windowedCounts = temperature.groupBy(
    window(temperature.Enqueued_Time, "15 minutes", "10 minutes"),
    temperature.timestamp).agg(F.max("value").alias("value")).orderBy(F.asc("timestamp"))
```

### Step 3. Create Function to call Anomaly Detector API and push data to Power BI Streaming Dataset. 
I will use writeStream with foreachbatch fucntion, this allows flexibility that we can to with data in each micro batch iteration 
In each micro batch I need to check if there are >=12 records, if yes we can call Anomaly Detection Last Point APT otherwise I need to wait. If I have enough records I can now call the API and get the response back. Once I get the reponse I can the push to Power BI stream dataset. 
```python
import requests
import json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from pyspark.sql.functions import lit,unix_timestamp
from pyspark.sql import functions as f 
from pyspark.sql.functions import to_json, spark_partition_id, collect_list, col, struct
# Import library to display results
import matplotlib.pyplot as plt
%matplotlib inline 
from pyspark.sql.functions import lit,unix_timestamp
import time
import datetime
from dateutil import parser

#Anomaly Detection
apikey = 'xxx' 
endpoint_latest = 'https://xxxx.cognitiveservices.azure.com/anomalydetector/v1.0/timeseries/last/detect'
#Power BI API
endpoint ='https://api.powerbi.com/beta/xxx...'

def detect(endpoint, apikey, request_data):
  headers = {'Content-Type': 'application/json', 'Ocp-Apim-Subscription-Key': apikey}
  response = requests.post(endpoint, data=json.dumps(request_data), headers=headers)
  if response.status_code == 200:
    return json.loads(response.content.decode("utf-8"))
  else:
    print(response.status_code)
    raise Exception(response.text)

        
def detect_anomaly(df):
  newdf=df[["timestamp", "value"]].dropDuplicates().sort(df.timestamp.asc())
  df2 = newdf.toJSON().map(lambda j: json.loads(j)).collect()
  single_sample_data = {}
  single_sample_data['series'] = df2
  single_sample_data['granularity'] = 'minutely'
  single_sample_data['maxAnomalyRatio'] = 0.25
  single_sample_data['sensitivity'] = 95
  single_point = detect(endpoint_latest, apikey, single_sample_data)
   
  result = {'expectedValues': [None]*len(df2), 'upperMargins': [None]*len(df2), 
              'lowerMargins': [None]*len(df2), 'isNegativeAnomaly': [False]*len(df2), 
              'isPositiveAnomaly':[False]*len(df2), 'isAnomaly': [False]*len(df2)}
  i=len(df2)  
  result['expectedValues'][i-1] = single_point['expectedValue']
  result['upperMargins'][i-1] = single_point['upperMargin']
  result['lowerMargins'][i-1] = single_point['lowerMargin']
  result['isNegativeAnomaly'][i-1] = single_point['isNegativeAnomaly']
  result['isPositiveAnomaly'][i-1] = single_point['isPositiveAnomaly']
  result['isAnomaly'][i-1] = single_point['isAnomaly']
  return result,single_sample_data

def sendToBi (data):
  data_str = data
  print(data_str)
  newHeaders = {'Content-type': 'application/json'}
  response = requests.post(endpoint,
                         data=data_str,
                         headers=newHeaders)
  return print("Status code: ", response.status_code)

## It spark dataframe
def convertdf (df):
  result = df.to_json(orient="records", date_format='iso', date_unit = 's')
  parsed = json.loads(result)
  return json.dumps(parsed) 

def callBI(result,single_sample_data):
  columns = {'expectedValues': result['expectedValues'], 'isAnomaly': result['isAnomaly'], 'isNegativeAnomaly': result['isNegativeAnomaly'],
              'isPositiveAnomaly': result['isPositiveAnomaly'], 'upperMargins': result['upperMargins'], 'lowerMargins': result['lowerMargins']
              , 'value': [x['value'] for x in single_sample_data['series']], 'timestamp': [parser.parse(x['timestamp']) for x in     single_sample_data['series']]}
  response = pd.DataFrame(data=columns)
  print(sendToBi(convertdf(response)))
  pass
  
def calldetector(df,epoch_id):
    df2=df.dropDuplicates(['timestamp'])
    if df2.count()>=12:
      result,single_sample_data= detect_anomaly(df2)
      callBI(result,single_sample_data)
    else:
      newdf=df2[["timestamp", "value"]]
      #df.withColumn("dt_truncated", date_trunc("second", col("dt")))
      df2 = newdf.toJSON().map(lambda j: json.loads(j)).collect()
      single_sample_data = {}
      single_sample_data['series'] = df2
      print(single_sample_data)
    pass 
```

### Step 4. Call Anomaly Detector API and Push Data:
Before run this command, ensure that you start your Raspberry Pi simulator and messages sent to IoT Hub
```python
(windowedCounts
  .writeStream
  .outputMode('complete')
  .foreachBatch(calldetector)
  .start().awaitTermination())
```

### Here how the result look like:
On Azure Databricks:
![alt text](https://github.com/WipadaChan/anomaly-detection/blob/main/image/Result.png)

On Power BI:
![alt text](https://github.com/WipadaChan/anomaly-detection/blob/main/image/PowerBIStreamDS.png)