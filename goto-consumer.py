import json
import os
import logging
import kafka_helper
from kafka import KafkaConsumer
import requests
import uuid
import time

logging.basicConfig(level=logging.INFO)

# CONSTANTS

TOPIC = os.environ.get('TOPIC', 'test')
KAFKA_PREFIX = os.environ.get('KAFKA_PREFIX', '')
TOPIC = KAFKA_PREFIX + TOPIC

logging.info('\nConsumer is using topic: ' + TOPIC + '\n')


class KafkaSimpleConsumer():
    def __init__(self):
        logging.info('Started consuming\n')        

        try: #heroku
            self.consumer = kafka_helper.get_kafka_consumer(topic=TOPIC)                        
            logging.info('RUNNING CONSUMER ON HEROKU\n') 

        except Exception: #locally on docker                           
            consumer = KafkaConsumer('test', bootstrap_servers='localhost:9092', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
            self.consumer = consumer
            logging.info('RUNNING CONSUMER LOCAL - so not using kafka_helper\n')

        if 'DATABASE_URL' in os.environ:
            self.use_db = True
            import psycopg2            
            DATABASE_URL = os.environ['DATABASE_URL']
            try:
                self.conn = psycopg2.connect(DATABASE_URL, sslmode='require')
                self.cur = self.conn.cursor()
                logging.info("Connected to the database")
            except:
                logging.info("I am unable to connect to the database")

    def write_to_pg(self, data):
        logging.info('Attempting to write to PG: ' + str(data))

        external_event_id = str(uuid.uuid4())
        pos_x__c = str(data['customer_id__c'])
        pos_y__c = str(data['customer_id__c'])
        patience__c = str(data['customer_id__c'])

        self.cur.execute("insert into salesforce.Captured_Event__c \
            (external_event_id__c, customer_id__c, payload__c) VALUES \
            (%s, %s, %s, %s)", (external_event_id, pos_x__c, pos_y__c, patience__c) )
        
        self.conn.commit()
    
    
    def start_listening(self):
        logging.info('\nListening for %s\n', TOPIC)    
        while True:
            logging.info('\nSleeping...\n')            
            time.sleep(5)            
            to_write = list()

            for msg in self.consumer:
                logging.info(msg.value)
                if type(msg.value) != dict:
                    payload = json.loads(msg.value)
                else:
                    payload = msg.value

                to_write.append(payload)
                 

                if self.use_db is True:
                    self.write_to_pg(payload)
            
            logging.info('\nAttempted to write to DB: %d rows', len(to_write))                       


if __name__ == '__main__':
    kafkaeske = KafkaSimpleConsumer()
    kafkaeske.start_listening()
