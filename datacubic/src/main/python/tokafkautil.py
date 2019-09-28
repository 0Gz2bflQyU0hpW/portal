#!/usr/bin/env python
# -*- coding: utf-8 -*- 
from kafka import KafkaProducer
from kafka import KafkaConsumer  
import json
import traceback
import logging


broker_list = 'first.kafka.dip.weibo.com:9092','second.kafka.dip.weibo.com:9092','third.kafka.dip.weibo.com:9092','fourth.kafka.dip.weibo.com:9092','fifth.kafka.dip.weibo.com:9092'

api_version = (0, 8, 2)

topic = 'dip-kafka2es-common'

class Kafka_Producer:

    def getConnect(self):
        self.producer = KafkaProducer(bootstrap_servers = broker_list, api_version = api_version)

    def sendjsondata(self,params):
        parmas_message = json.dumps(params)
        self.producer.send(topic, parmas_message.encode('utf-8'))

    def disConnect(self):
        self.producer.close()

class Kafka_Consumer:  
  
    def getConnect(self):  
        self.consumer = KafkaConsumer(topic, bootstrap_servers = broker_list)  
  
    def beginConsumer(self):    
        for oneLog in self.consumer:    
            print(oneLog)    
  
    def disConnect(self):  
        self.consumer.close()  
 
