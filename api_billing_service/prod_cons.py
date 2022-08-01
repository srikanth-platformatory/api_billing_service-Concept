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
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
from pprint  import pprint
from confluent_kafka import DeserializingConsumer

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# used to consumer json values from api_logs_json and produce avro records to api_logs_avro


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    conf1 = conf.copy()
    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_367'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer_conf['key.deserializer'] = StringDeserializer()
    consumer = DeserializingConsumer(consumer_conf)

    print(conf1)

    schema_registry_conf = {
        'url': conf1['schema.registry.url'],
        'basic.auth.user.info': conf1['basic.auth.user.info']}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    query_string_list = []
    count = 0

    query_dict = []

    value_schema_str = """
                {
                "name": "MyClass",
                "type": "record",
                "namespace": "com.acme.avro",
                "fields": [
                    {
                    "name": "consumer",
                    "type": {
                        "name": "consumer",
                        "type": "record",
                        "fields": [
                        {
                            "name": "created_at",
                            "type": "int"
                        },
                        {
                            "name": "id",
                            "type": "string"
                        },
                        {
                            "name": "username",
                            "type": "string"
                        }
                        ]
                    }
                    },
                    {
                    "name": "request",
                    "type": {
                        "name": "request",
                        "type": "record",
                        "fields": [
                        {
                            "name": "headers",
                            "type": {
                            "name": "headers",
                            "type": "record",
                            "fields": [
                                {
                                "name": "consumed_units",
                                "type": "string"
                                },
                                {
                                "name": "subscription_id",
                                "type": "string"
                                }
                            ]
                            }
                        }
                        ]
                    }
                    },
                    {
                    "name": "response",
                    "type": {
                        "name": "response",
                        "type": "record",
                        "fields": [
                        {
                            "name": "status",
                            "type": "int"
                        }
                        ]
                    }
                    },
                    {
                    "name": "request_access_time",
                    "type": "long"
                    }
                ]
            }
        """

    
    #print(value_avro_serializer)

    # for full list of configurations, see:
    #  https://docs.confluent.io/platform/current/clients/confluent-kafka-python/#serializingproducer

    value_avro_serializer = AvroSerializer(schema_registry_client = schema_registry_client,
                                           schema_str =  value_schema_str)

    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf1)
    producer_conf['key.serializer'] = StringSerializer('utf_8')
    # producer_conf['key.serializer'] = key_avro_serializer
    producer_conf['value.serializer'] = value_avro_serializer

    p = SerializingProducer(producer_conf)

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
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()

                record_value = msg.value()
                data = json.loads(record_value)
                
                print(record_key)
                print()
                pprint(data, indent=3)


                # ----

                new_data = {
                            "consumer":data["consumer"],
                            "request": {"headers": {
                                                    "consumed_units": data['request']['headers']['consumed_units'] , 
                                                    "subscription_id": data['request']['headers']['subscription_id'] 
                                                } 
                                        },
                            "response": {
                                "status":data['response']['status']
                                    },
                            "request_access_time": data["request_access_time"]
                            }

                # new_data['consumer'] = data['consumer']
                # new_data['request'] = {"headers": {
                #                                     "consumed_units": data['request']['headers']['consumed_units'] , 
                #                                     "subscription_id": data['request']['headers']['subscription_id'] 
                #                                 } 
                #                         },
                # new_data['response'] = {
                #                         "status":data['response']['status']
                #                         }
                
                print()
                pprint(new_data)


                produce_topic = "api_logs_avro_"

                p.produce(produce_topic, key=record_key, value=new_data, on_delivery=acked)
    
                p.flush()
                # subscription_id = data['subscription_id'].strip()
                # subscription_for = data['subscription_for'].strip()
                # subscription_from = data['subscription_duration']['from'].strip()
                # subscription_to = data['subscription_duration']['to'].strip()
                # field_reference = data['subscription_plan']['metering_units']['field_ref'].replace("response.headers.", '').strip()
                # aggregation_type = data['subscription_plan']['metering_units']['aggregated_as'].strip().upper()
                # subscription_id_source = data['subscription_plan']['subscription_id_source'].replace("response.headers.", '').strip()

                # print(subscription_id, subscription_for, subscription_from, subscription_to, field_reference, aggregation_type, subscription_id_source)

                # query = f"""select subscription_id, {aggregation_type}({field_reference}),  from api_logs_avro where now() between '{subscription_from}' AND '{subscription_to}' WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY subscription_id ,x ,y, z ;"""

                # query_dict_item = {'subscription_id': subscription_id, 'query': query}
                # query_dict.append(query_dict_item)

                # query_string_list.append(query)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

    # print(query_dict, '\n', type(query_dict))
    # with open("queries.ksql", 'w') as f:
    #     # f.write('\n'.join(query_dict))
    #     json.dump(query_dict, f)




# ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-events
# ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-payload
#  ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-specs