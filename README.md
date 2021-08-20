# Caching alternatives in Google Dataflow

Github repo with the sample source code used for the blog post about caching alternatives in Dataflow.

## Before running the code
* VPC. For running the sample code default VPC would be Ok but feel free to create a new one and pass as part of the parameters to the Dataflow job.
* GCP bucket for staging files and serve as temp location
* Pubsub topic and subscription for that topic
* Service account with dataflow worker permissions, storage admin access to GCS, MemoryStore editor and PubSub subscriber. 
* A memory store instance (Required for running the cache version of the job)

## Setting up the Memorystore instance
```
# Enable de API
gcloud services enable memcache.googleapis.com 

#Create the cache instance
gcloud beta memcache instances create df-cache --node-count=1 --node-cpu=1 --node-memory=1GB --region=us-west1

#Get instance details
gcloud beta memcache instances describe df-cache --region=us-west1
```
## Running the caching option
Open the file df_cache_run.sh in the root directory and change the following parameters:
| Param | Description |
|-------|-------------|
| subnetwork | Name of the subnet used for running worker nodes. If you created a new VPC with subnets replace it with that reference. |
| serviceAccount | Replace it with the service account that was created and has the required permissions for running the sample code. |
| project | GCP project ID |
| stagingLocation | Set the bucket name used by dataflow for staging files.|
| tempLocation | Set the bucket name used by dataflow for temp files. The same bucket for staging location can be used. |
| subscription | Pubsub subscription reference from where the job will read messages. |
| dataBucket | Bucket where output data will be written. |
| cacheEndpoint | Host and port of the cache instance. |
| cacheTTL | TTL of the cached service response |
| weatherApiKey | Generated API key from OpenWeather site |
| Region | Region where the job should run. |

Once the settings were changed execute the script and wait for the job to be ready.

## Running the stateful option
Open the file df_stateful_run.sh in the root directory and change the following parameters:

| Param | Description |
|-------|-------------|
| subnetwork | Name of the subnet used for running worker nodes. If you created a new VPC with subnets replace it with that reference. |
| serviceAccount | Replace it with the service account that was created and has the required permissions for running the sample code. |
| project | GCP project ID |
| stagingLocation | Set the bucket name used by dataflow for staging files. |
| tempLocation | Set the bucket name used by dataflow for temp files. The same bucket for staging location can be used. |
| subscription | Pubsub subscription reference from where the job will read messages. |
| dataBucket | Bucket where output data will be written. |
| maxStateInterval | Maximun time in ms that objects are kept in storage before performing external service invocation |
| triggerEveryCount | Minimun number of elements to buffer before trigger processing |
| triggerEveryTimeSec | Time elapsed before triggering processing in seconds |
| weatherApiKey | Generated API key from OpenWeather site |
| Region | Region where the job should run. |

Once the settings were changed execute the script and wait for the job to be ready.

## Publishing messages for the job
The format used is very simple:
```
{ 
    "indoorTemp":12.5, 
    "cityCode":"La Plata",
    "countryCode":"AR"
}
```
You can use any city name and country code but be aware that has to be a valid combination otherwise if going to fail.

The class [PubsubPublisher](src/test/java/com/pythian/pipeline/test/utils/PubsubPublisher.java) would be useful for publishing massive messages and make a more intensive testing around these pipelines.







