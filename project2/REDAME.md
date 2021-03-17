# Project 2: SF Crime

## Command for execution

Each of the following commands needs to be run in a different terminal window:

```/usr/bin/zookeeper-server-start config/zookeeper.properties```
```/usr/bin/kafka-server-start config/server.properties```
```python kafka_server.py```

The Structured streaming application is started like this:

```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py```

Console output of kafka consumer:

```/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic com.udacity.spark.sf.policecalls --from-beginning```

## Answers


1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

A prominent lever to adjust throughput is `processedRowsPerSecond`. Once arriving a the resource limit memory and number of cores allocated will start to have an impact as well as network capacity.

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

The most effectfull levers seem to be `spark.default.parallelism` and `spark.streaming.kafka.maxRatePerPartition`. however there is a trade off between number of processes and networking due to re shuffling. Spark console progress information shows performance metrics in terms of `numInputRows`, `inputRowsPerSecond`, and `processedRowsPerSecond`.