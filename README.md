Real Time Streaming Pipeline for Various Securities.

Currently targeting real time volumes for Cryptos and Securities and Macros.

Current tentative stack:

Streaming ingestion via Spark, storage in Iceberg Tables. 
Brokering with Kafka with visualizations in Grafana. Considering playing with Confluent.

Ingestion from APIs (Currently considering Polygon or Tradier), multiple securities fed to a broker. Quality checks performed on data completion.

Success Metrics:
Table that can be updated and custom metrics and models developed from. Exploring the relationships between securities and various macro factors.


Realtime Streaming Data


Polygon API -> Timescale DB 