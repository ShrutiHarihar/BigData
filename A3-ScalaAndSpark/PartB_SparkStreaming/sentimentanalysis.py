from nltk.sentiment.vader import SentimentIntensityAnalyzer
from kafka import SimpleProducer, KafkaClient, KafkaConsumer
from elasticsearch import Elasticsearch
from _collections import defaultdict
import json

elasticIndex = "sentiment"
consumer = KafkaConsumer('twitterstream','localhost:9092')
es = Elasticsearch()
if not es.indices.exists(elasticIndex):  # create if the index does not exist
    es.indices.create(elasticIndex)

dType = "hw"
mapping = {
            dType: {
                "properties": {
                 "sentiment":{"type": "string"},
                "tagType":{"type": "integer"},
                "message":{"type": "string", "index": "not_analyzed"},
                "hashtags":{"type": "string", "index": "not_analyzed"},
              }
            }
        }
tagType = 0

def main():
    vader =  SentimentIntensityAnalyzer()
    for message in consumer:
         print(message)
         score = vader.polarity_scores(message)
         print(score)
         if score["compound"] < 0:
            sentiment = "negative"
         elif score["compound"] == 0:
             sentiment = "neutral"
         else:
             sentiment = "positive" 
       
         if "obama" in tweet_text:
             tagType = 1
         elif "trump" in tweet_text:
             tagType = 2
         content = {       
                      "sentiment":sentiment,
                      "message": message,
                      "tagType":tagType
				  }
         es.indices.put_mapping(index=elasticIndex, doc_type=dType, body=mapping)
         es.index(index=elasticIndex, doc_type=dType, body=content)
         es.indices.refresh(index="sentiment")
					
if __name__ == "__main__":
    main()