# Real-time anomaly detection with Azure Anomaly Detector API with Spark Structure Streaming 
This repo shows how you can use Anomaly Detector API with Streaming data feed in Azure Databricks. 
The Anomaly Detector API enables you to monitor and find abnormalities in your time series data by automatically identifying and applying the correct statistical models, regardless of industry, scenario, or data volume. Using your time series data, the API can find anomalies as a batch throughout your data, or determine if your latest data point is an anomaly.

## Prerequisites

1. You must have an [Anomaly Detector API resource](https://aka.ms/adnew). Before continuing, you will need the API key and the endpoint from your Azure dashboard.
   ![Get Anomaly Detector access keys](./media/cognitive-services-get-access-keys.png "Get Anomaly Detector access keys")
2. Azure Databrick cluster (up and running). To create Azure Databrick cluster, plese refer to https://docs.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal  
3. IoT Hub endpoint for feeding [Raspberry Pi Simulation data](https://azure-samples.github.io/raspberry-pi-web-simulator/)

## Overview Architecture 

![alt text](https://github.com/WipadaChan/anomaly-detection/blob/main/image/overview.png "Overview Architecture") 


