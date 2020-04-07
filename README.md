# Leads Pipeline

Apache Beam Pipeline that reads data from GCP Pub/Sub, applies some validation and writes the data to Google Cloud Storage.

## Execution

To run the pipeline:

```shell script
mvn compile exec:java -Dexec.mainClass=br.com.dr.leads.pipeline.LeadsPipeline -Dexec.args=" \
--runner=DataflowRunner \
--streaming=true \
--project=<PROJETCT-ID> \
--subscription=<SUBSCRIPTION> \
--jobTitlesCsvPath=<JOB-TITLES-CSV-PATH> \
--outputRootPath=<OUTPUT-PATH> \
-Pdataflow-runner
```

To produce data you can use this [script](./scripts/publisher.rb), but, before executing it:
- change the `<PROJECT-ID>` and `<TOPIC_NAME>` placeholders for your project and topic.
- install Ruby and [google/cloud/pubsub gem](https://github.com/googleapis/google-cloud-ruby/tree/master/google-cloud-pubsub).  