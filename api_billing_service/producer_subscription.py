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


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)


    # schema registry client
  #   schema_registry_conf = {
  #       'url': conf['schema.registry.url'],
  #       'basic.auth.user.info': conf['basic.auth.user.info']}

  #   schema_registry_client = SchemaRegistryClient(schema_registry_conf)


  #   value_schema_str="""
  #   {
  #   "name": "MyClass",
  #   "type": "record",
  #   "namespace": "com.acme.avro",
  #   "fields": [
  #     {
  #       "name": "subscription_id",
  #       "type": "string"
  #     },
  #     {
  #       "name": "subscription_for",
  #       "type": "string"
  #     },
  #     {
  #       "name": "subscription_duration",
  #       "type": {
  #         "name": "subscription_duration",
  #         "type": "record",
  #         "fields": [
  #           {
  #             "name": "from",
  #             "type": "string"
  #           },
  #           {
  #             "name": "to",
  #             "type": "string"
  #           }
  #         ]
  #       }
  #     },
  #     {
  #       "name": "subscription_plan",
  #       "type": {
  #         "name": "subscription_plan",
  #         "type": "record",
  #         "fields": [
  #           {
  #             "name": "name",
  #             "type": "string"
  #           },
  #           {
  #             "name": "id",
  #             "type": "string"
  #           },
  #           {
  #             "name": "type",
  #             "type": "string"
  #           },
  #           {
  #             "name": "tiers",
  #             "type": {
  #               "type": "array",
  #               "items": {
  #                 "name": "tiers_record",
  #                 "type": "record",
  #                 "fields": [
  #                   {
  #                     "name": "up_to",
  #                     "type": "int"
  #                   },
  #                   {
  #                     "name": "rate",
  #                     "type": "float"
  #                   }
  #                 ]
  #               }
  #             }
  #           },
  #           {
  #             "name": "metering_units",
  #             "type": {
  #               "name": "metering_units",
  #               "type": "record",
  #               "fields": [
  #                 {
  #                   "name": "type",
  #                   "type": "string"
  #                 },
  #                 {
  #                   "name": "field_ref",
  #                   "type": "string"
  #                 },
  #                 {
  #                   "name": "aggregated_as",
  #                   "type": "string"
  #                 }
  #               ]
  #             }
  #           },
  #           {
  #             "name": "bill_criteria",
  #             "type": {
  #               "type": "array",
  #               "items": {
  #                 "name": "bill_criteria_record",
  #                 "type": "record",
  #                 "fields": [
  #                   {
  #                     "name": "expresssion",
  #                     "type": "string"
  #                   }
  #                 ]
  #               }
  #             }
  #           },
  #           {
  #             "name": "subscription_id_source",
  #             "type": "string"
  #           }
  #         ]
  #       }
  #     }
  #   ]
  # }
  #   """

  #   value_avro_serializer = AvroSerializer(schema_registry_client = schema_registry_client,
  #                                          schema_str =  value_schema_str)

    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)

    # producer_conf['key.serializer'] = StringSerializer('utf_8')
    # producer_conf['value.serializer'] = value_avro_serializer

    # Create Producer instance
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

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

    # previous====== 
    # with open('data.txt', 'r') as data_file:
    #     json_data = data_file.read()

    # data = json.loads(json_data)
    # ================================



    # for new subscription data, use subscriptions1.json
    # file already in json format

    # with open("subscription1.json", "r") as data_file:
    #     data = data_file.read()

    # for subscription1.txt
    with open('subscription1.txt', 'r') as data_file:
        json_data = data_file.read()

    data = json.loads(json_data)


    for x in data:
        record_key = x["subscription_id"]
        record_value = json.dumps(x)
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)
    
    producer.flush()
    


    print("{} messages were produced to topic {}!".format(delivered_records, topic))


    # ./producer_new.py -f ./new_config.config -t subscriptions_json <- old topic

    # ./producer_subscription.py -f ./new_config.config -t demo_subscriptions_json