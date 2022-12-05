# Benchmark time-series database on varied data types

## Datasets

### Download
The following link contains all datasets to be added into the Apache IoTDB
https://cloud.tsinghua.edu.cn/f/7e6ab80cac6845b8b6c0/?dl=1

### import
tools/import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f xxx.csv

## Baseline v.s. Optimization

Baseline: IoTDBXXUDF

Optimization: IoTDBXX

XX = {TS, Temporal, Relational}

## Time-series query

Use App.java > IoTDBTS or IoTDBTSUDF


## Series-temporal query

Use App.java > IoTDBTemporal or IoTDBTemporalUDF

## Series-relational query

Use App.java > IoTDBRelational or IoTDBRelationalUDF