# Databricks notebook source

# COMMAND ----------

import requests
import json
instance_id = "dbc-e55d56e6-6512.cloud.databricks.com"
api_version = '/api/2.1'
api_command = '/jobs/create'
url = f"https://{instance_id}{api_version}{api_command}"
params = {
 "Authorization" : "Bearer dapiddd37aacca39507c033f9dcbcd52ff01",
 "Content-Type" : "application/json"
 }
wf_json=json.open(../workflows/)
response = requests.post(
url = url,
headers = params,
data = json.dumps(wf_json) # converting payload to json
)
#print(response.text)
print(json.dumps(json.loads(response.text), indent = 4))

# COMMAND ----------