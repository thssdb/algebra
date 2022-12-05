# Benchmark time-series database on varied data types

## 1 Datasets

### 1.1 Download
The following link contains all datasets to be added into the Apache IoTDB
https://cloud.tsinghua.edu.cn/f/7e6ab80cac6845b8b6c0/?dl=1

### 1.2 import
tools/import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f xxx.csv

## Routine

### 2.1 Fetch IoTDB

If you use baseline artifact already built, choose apache-iotdb-0.13.3

https://github.com/apache/iotdb/releases/download/v0.13.3/apache-iotdb-0.13.3-all-bin.zip

Otherwise, you could build apache-iotdb on ur own and use the udf library in
https://github.com/apache/iotdb/tree/research/algebra

### 2.2 add baseline artifacts to iotdb runtime

2.2.1 create udf path:

mdir ../xxx(iotdb)-bin/ext/udf

2.2.2 mv artifacts into the udf path

mv ./artifacts/library-udf-0.14.0-SNAPSHOT-jar-with-dependencies.jar ../xxx(iotdb)-bin/ext/udf/

### 2.3 start server

../xxx(iotdb)-bin/sbin/start-server.bat (or sh for linux-based sys.)


### 2.4 import data

Please refer to 1.2: import to import all datasets 
For example, wind.csv
> result: "import successfully!"

### 2.5 see how data imported from CLI

2.5.1 Launch a new cmd/shell

../xxx(iotdb)-bin/sbin/start-cli.bat (sh for Linux...)

2.5.2 Input SQL:

> "show timeseries;"

### 2.6 use benchmark (this repo)

See the following sections.
The performance data will be shown in three dictionaries

> ./latency-ts/ts or udf

> ./latency-temporal/ts or udf

> ./latency-relational/ts or udf

Please use mdir to create them ahead of benchmarking.

## 3 Baseline v.s. Optimization

Baseline: IoTDBXXUDF

Optimization: IoTDBXX

XX = {TS, Temporal, Relational}

## 4 Time-series query

Use App.java > IoTDBTS or IoTDBTSUDF


## 5 Series-temporal query

Use App.java > IoTDBTemporal or IoTDBTemporalUDF

## 6 Series-relational query

Use App.java > IoTDBRelational or IoTDBRelationalUDF