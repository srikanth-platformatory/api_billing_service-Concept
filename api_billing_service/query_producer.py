#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


delivered_records = 0

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

def query_producer(key, msg, topic):

    # args = ccloud_lib.parse_args()
    # config_file = args.config_file
    # topic = args.topic
    # conf = ccloud_lib.read_ccloud_config(config_file)

    producer_conf = {}
    producer_conf['bootstrap.servers'] = "pkc-7prvp.centralindia.azure.confluent.cloud"
    producer_conf['security.protocol'] = "SASL_SSL"
    producer_conf['sasl.mechanisms'] = "PLAIN"
    producer_conf['sasl.username'] = "BH2H2NXOJQPDICEW"
    producer_conf['sasl.password'] = "zgYJtRE+3RUcr5lySGBx806xJDV3Kg7GfGqXAX3c4bQf4NTnK4secW2LHc14u6k7"
    producer_conf['session.timeout.ms'] = 45000

    
    # Best practice for higher availability in librdkafka clients prior to 1.7
    # session.timeout.ms=45000
    # Confluent Cloud Schema Registry
    # schema.registry.url=https://psrc-5mn3g.ap-southeast-2.aws.confluent.cloud
    # basic.auth.credentials.source=USER_INFO
    # basic.auth.user.info=63MFAU73MHENGQQ4:WjTxZrBu25UTPuMNZas0/KyC3YgIszpSh1W45DqYH096rwISCnNFmpqIg26PS6YH




    # producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)

    # Create Producer instance
    producer = Producer(producer_conf)

    # Create topic if needed
    # ccloud_lib.create_topic(conf, topic)

    producer.produce(topic= topic, key=key, value=msg, on_delivery=acked)
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))



# if __name__ == '__main__':

#     # Read arguments and configurations and initialize
#     args = ccloud_lib.parse_args()
#     config_file = args.config_file
#     topic = args.topic
#     conf = ccloud_lib.read_ccloud_config(config_file)


#     producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)

#     # Create Producer instance
#     producer = Producer(producer_conf)

#     # Create topic if needed
#     ccloud_lib.create_topic(conf, topic)

#     delivered_records = 0

#     # Optional per-message on_delivery handler (triggered by poll() or flush())
#     # when a message has been successfully delivered or
#     # permanently failed delivery (after retries).
#     def acked(err, msg):
#         global delivered_records
#         """Delivery report handler called on
#         successful or failed delivery of message
#         """
#         if err is not None:
#             print("Failed to deliver message: {}".format(err))
#         else:
#             delivered_records += 1
#             print("Produced record to topic {} partition [{}] @ offset {}"
#                   .format(msg.topic(), msg.partition(), msg.offset()))

#     with open('queries.ksql', 'r') as data_file:
#         json_data = data_file.read()

#     data = json.loads(json_data)

#     for x in data:

#         record_key = x["subscription_id"]
#         record_value = x["query"]

#         print("Producing record: {}\t{}".format(record_key, record_value))
#         producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
#         producer.poll(0)
    
#     producer.flush()
    

#     # for n in range(10):
#     #     record_key = "alice"
#     #     record_value = json.dumps({'count': n})
#     #     print("Producing record: {}\t{}".format(record_key, record_value))
#     #     producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
#     #     # p.poll() serves delivery reports (on_delivery)
#     #     # from previous produce() calls.
#     #     producer.poll(0)

#     # producer.flush()

#     print("{} messages were produced to topic {}!".format(delivered_records, topic))


#     # ./producer_new.py -f ./new_config.config -t tester