#!/usr/bin/env bash

mvn compile exec:java \
 -Dexec.mainClass=com.pythian.pipeline.PubSubExternalAPICallStatefullOption \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --subnetwork='<REPLACE WITH SUBNET RED>' \
 --serviceAccount=<REPLACE WITH SERVICE ACCOUNT> \
 --project=<REPLACE WITH PROJECT NAME> \
 --stagingLocation=gs://<REPLACE WITH JOB BUCKET>/staging \
 --tempLocation=gs://<REPLACE WITH JOB BUCKET>/temp \
 --runner=DataflowRunner \
 --subscription=<REPLACE WITH SUBSCRIPTION NAME> \
 --dataBucket=gs://<REPLACE WITH DATA BUCKET> \
 --maxStateInterval=60 \
 --triggerEveryCount=3 \
 --triggerEveryTimeSec=60 \
 --weatherApiKey=<REPLACE WITH THE WEATHER API KEY> \
 --region=us-west1 \
 --numWorkers=2 \
 --maxNumWorkers=10 \
 --autoscalingAlgorithm=NONE \
 --diskSizeGb=500 \
 --workerMachineType=n1-highmem-8"
