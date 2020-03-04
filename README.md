# pageviews
<h5>What additional things would you want to operate this application in a production setting?</h5>

Assuming there's more functionality to justify the below.

1. Front service with restful api (e.g. Tomcat/Jersey)
    
    Probably host on AWS

2. Configurability
    
    Lot of hardcoded things which you wouldn't want in a production environment. First thing would be to move those configs out to be managed by something like puppet.
3. Retries

    Better exception handling, for e.g. downloads could fail for any number of reason and you'd probably want to investigate what kind of retry strategies would work best.
    
4. High Availability

    Distribute the service across different VM's or kubernetify it.
    
5. Switch out the execution engine

    I wouldn't use a thread pool to execute the downloads/process tasks. These are ideal candidates for some form of distributed execution engine such as spark that abstracts away the complexities of auto scaling (if with kubernetes), scheduling and task orchestration.

6. Move cached results to durable stores

    I/O interactions are complicated that I'd usually want delegate. You'd want to cache these results in something that gives durability/redundancy such as a DB (ES, Cassandra) or HDFS (S3).

7. Decompose

    With expanded functionality I'd consider breaking this out into multiple services. I'd initially look at breaking out the agent side of this application (i.e. the fetching of files) and a service that parses/filters/processes/aggregates the resources.
     
<h5>What might change about your solution if this application needed to run automatically for each hour of the day?</h5>

I'd probably look into integrating some form of job scheduler like jenkins to auto run this. I'd also look to see what kind of real time sources there are (I did notice there was one available) and have near real time execution of this.

<h5>How would you test this application?</h5>

Unit tests, outside of the obvious tests for the parsing, filtering and aggregation. I'd look to write tests for the different failure cases, of the top of my head
1) Failed/partial downloads
2) Malformed inputs
3) Failed aggregations

<h5>How you’d improve on this application design?<h5>

   At a high level, I think migrating this over to use a distributed compute system such as spark makes the most sense. This type of problem works well with spark as a lot of the complexities around parallel processing are abstracted away. The main thing I'd look into is a better way of ingesting the data, GZIP not being splittable means you miss out on a lot of optimizations that spark will get you. The kind of pipeline I'd look at implementing with spark would be, download -> decompress to parquet -> spark filter/aggregate. 
   
   Depending on the types of queries this service would get I'd also look at caching things into some fast access layer (e.g. elasticsearch, sql etc). Elasticsearch looks like a good candidate here as it's a read only workflow with timeseries data. Assuming you had other types of queries, you could even store the page views in elasticsearch and let ES do the aggregation. There are two things that would make this quite performant
   1) Index per time bucket (i.e. index per day, week or month etc), this means that you could skip reading/process entire indexes based on your query
   2) ES caching, for aggregations specifically the shard request cache would save a lot from subsequent aggregation calls.
   
   With larger scale/more insight on query patterns you could justify using a combination of both for hot/cold access.
   
   Another way to model this is as a multi-stage pipeline e.g. download -> sanitise -> parse -> filter -> process, where each "stage" in the pipeline would be an independent producer/consumer, that can subscribe to any compatible producer. This allows for a number of nice functionalities, for e.g.
    1) You could start processing files as soon as they're done as opposed to waiting for all files to download
    2) You could have multiple consumers performing different things in parallel, e.g. in this particular solution the file is first downloaded, then read/parsed. With a stream you could have a consumer that sinks to a file and another that parses the input stream.

   Additionally, given more functionality, I can imagine a number of additional functionalities (e.g. different metrics) where you may want to fork off at various points of your pipeline topology to do different things. This would make it quite extensible as producers of things don't need to worry about consumers of it.
   
   You could do the above using Kafka as the messaging layer and flink as the execution engine.