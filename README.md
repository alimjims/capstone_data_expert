**Data Expert Capstone: Real Time Stock Streaming Project***

As part of my continued learning journey, I wanted to expand on my working knowledge of streaming pipelines, the common tools and their uses cases. 

This project makes use of Polygon.io's Websocket for public market stocks that provides a steady stream of stock data on multiple different time aggregates and dimensions when markets are open, the API also provides historical data for more batch related projects if needed. For testing during off market hours, a dummy stock data generator that mimics the behavior of Polygon.io's websocket was made as the websocket for aggregates was not available at all hours.

Ultimately the goal was to see how we can generate custom metrics that are not readily available from APIs not only on a one off basis, but on a repeatable basis while also handling a stream of data. When analyzing securities of any type, generating alpha or uncorrelated returns generally requires you to cut and slice data in ways that are not always straightforward to pull from available datasources and data streams. This project was meant to broaden my skillsets by developing a pipeline that utilizes essentialy streaming technologies with datasets in a context I was unfamiliar.

The technologies utilized downstream for handling this streaming data are Kafka and TimescaleDB and Grafana was utilized as the visualization layer. 

**Confluent Kafka**
Kafka serves a fault tolerant and independent layer between the websockets/apis and the consumers the pipeline has downstream, allowing for high-throughput data streams and scaling. While there was not neccesarily a need for Kafka here given that we dont have a large amount of consumers currently reading the data, as this project grows, Kafka and Confluent will also for smooth scaling of this project while also allowing for the different modules to remain decoupled from each other. 

![confluent_kafka](https://github.com/user-attachments/assets/c2e59095-6108-4559-92c2-460877574080)


**TimescaleDB**
The landing zone for data from our Kafka topics was TimescaleDB which I ultimately decided to utilize in this project because of its compatability with time-series data. Many databases struggle with high-throughput time series data natively. TimescaleDB solves this by automatically partitioning tables by time (hypertables), compressing old data, and providing time-series specific functions—all while maintaining full PostgreSQL compatibility. 

![jobs_timescale](https://github.com/user-attachments/assets/1925ac34-e5f9-4b89-9c25-1a88d4ae491d)

From the Kafka Topic, we utilize a Consumer to read data from the topic(s) into TimescaleDB hypertables, this base table is the main table that our main views our generated off of.  A suite of tests are ran on both the base table and the interim base views being generated with the final views being updated on a cadence of every 5 seconds with its continuous aggregate functions. Given TimescaleDB's optimization for time series data, queries are able to be run incredibly quickly every few seconds and sent to downstream processes. 

![image](https://github.com/user-attachments/assets/dfa8bfc4-046b-468e-8384-81b34660e469)

**Grafana**
For visualizations, Grafana was chosen because of its compatability with time series data and also its ease of connection with TimescaleDB databases and tables and its ability to refresh every 1-5s depending on the version being utilized. 



(https://github.com/user-attachments/assets/febdd9fc-595d-4144-923e-ff0e8df5902b)


**What's Next**
I hope to expand on this project in the coming week by implementing data transformations outside of SQL transformations in TimeScale such as Flink or Spark streaming. I also hope to utilize Terraform to set up the environment for this. 

