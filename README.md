# Collection-Engine

## Overview
Collection Engine is a program written in Go mimicing an ETL pipeline. It leverages Go's built in goroutines to manage concurrency with little overhead.
The engine has 4 main components:
- Source Service
  - a client handles requests to the upstream data source API and handles any HTTP errors
  - if errors are encountered, the client waits 500ms before reissuing the request
  - the client sends successful responses into a Messages channel which has a configurable number of consumers
  - if the Messages channel has no ready consumers, the stops making requests to the data source until a consumer is ready
- Processing Service
  - service is made up of a configurable number of consumers who will pull data from the upstream Messages channel
  - consumers issue requests to the processing API
  - if an error is returned from the processing API, the consumer sends the job to the Retries channel
  - successful responses received from the processing API are added to the ProcessedMessages channel
- Storage Service
  - similar to the Processing Service, this has a configurable number of consumers pulling data from the ProcessedMessages channel
  - consumers make requests to the Storage Service API
  - if an error is received from the Storage API, the job is sent to the Retries channel
  - if a success is returned, success is logged and no further action is taken
- Retry Service
  - single goroutine retrying failed jobs from the Retries channel
  - retries 2 times for a total of 3 attempts before giving up and logging failure

### Additional Thoughts
Could refactor the processing and storage services into a single service to DRY up the code. Quite a few things are hardcoded (like the backoff strategy for the Source Service), if I had a better understanding of the upstream data source and what to expect I would readdress that strategy. I wish I had more experience with helm and deploying to kubernetes clusters since once I got to that step, I had to go back and rethink a few of the ways I was setting up the application.


## Setup
1. add configuration values in `helm/config.yaml`:
```
defaultClientTimeout: 5s
defaultWorkersCount: 3

sourceApiBaseUrl: "https://example.com"
sourceApiAuthToken: "example"
sourceClientTimeout: 5s
sourceApiRateLimit: 120
sourceApiRateLimitPeriodSecs: 60

processingApiBaseUrl: "https://example2.com"
processingClientTimeout: 7s
processingWorkersCount: 4


storageApiBaseUrl: "https://example3.com"
storageClientTimeout: 10s
storageWorkersCount: 2
```
2. The helm chart points to a docker image hosted publicly so you shouldn't need to build or host the image unless you prefer. Ff you have a kubernetes cluster running, deploy to the cluster with helm `helm install <release_name> ./helm/`


1. If instead you want to build the docker image and host elsewere, 
2. Build image `docker build -t <tag_name> .`
3. Push to an image repository such as Dockerhub and be sure to update `image.hostname` and `image.repository` in the `helm/values.yaml` file

## Testing
1. Run all tests `go run -race ./engine/...`, include `-v` flag for log output
2. Run specific test file `go run -race ./engine/<filename>_test.go`
3. Test coverage `go test -coverprofile cover.out ./engine/...`
4. View coverage report in browser `go tool cover -html=cover.out`