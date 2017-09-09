Add your Twitter credentials in twitter-app-credentials.txt
Steps to run Dynamic streaming
1. Start the following servers
	a) Zookeeper
	b) Kafka
	c) Elastic Search
	d) Kibana
2. Run the Kafka Producer(app.py) in pyspark
3. Run the Kafka Consumer(sentimentanalysis.py) in pyspark
4. Open the kibana server in local browser(localhost:5601) and see the visualizations