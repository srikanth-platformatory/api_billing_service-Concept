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



from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.avro.serializer import SerializerError,  KeySerializerError, ValueSerializerError

import json
import ccloud_lib


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    schema_registry_conf = {
        'url': conf['schema.registry.url'],
        'basic.auth.user.info': conf['basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)


    value_schema_str = """
                {
                "fields": [
                    {
                    "default": null,
                    "name": "VALUE",
                    "type": [
                        "null",
                        "int"
                    ]
                    },
                    {
                    "default": null,
                    "name": "START_TIME",
                    "type": [
                        "null",
                        "long"
                    ]
                    },
                    {
                    "default": null,
                    "name": "END_TIME",
                    "type": [
                        "null",
                        "long"
                    ]
                    }
                ],
                "name": "KsqlDataSourceSchema",
                "namespace": "io.confluent.ksql.avro_schemas",
                "type": "record"
                }
    """

    # key_schema_string="""
    #             {
    #         "doc": "Sample schema for the key of the record.",
    #         "fields": [
    #             {
    #             "doc": "This is the key",
    #             "name": "subscription_id",
    #             "type": "string"
    #             }
    #         ],
    #         "name": "KsqlDataSourceSchema",
    #         "namespace": "io.confluent.ksql.avro_schemas",
    #         "type": "record"
    #         }
    # """

    # key_avro_deserializer = AvroDeserializer(schema_registry_client = schema_registry_client , schema_str = key_schema_string)
    value_avro_deserializer = AvroDeserializer(schema_registry_client = schema_registry_client, schema_str = value_schema_str)

    # for full list of configurations, see:
    #   https://docs.confluent.io/platform/current/clients/confluent-kafka-python/#deserializingconsumer
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    # consumer_conf['key.deserializer'] = key_avro_deserializer
    consumer_conf['value.deserializer'] = value_avro_deserializer
    consumer_conf['group.id'] = 'python_example_group_new_100'
    consumer_conf['auto.offset.reset'] = 'earliest'
    # consumer_conf['enable.auto.commit'] = False
    consumer = DeserializingConsumer(consumer_conf)
    
    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0

    while True:
        try:

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
                key_object = msg.key()
                value_object = msg.value()
                total_count += 1
                # print(total_count)
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(key_object, value_object, total_count))
                
        except KeyboardInterrupt:
            break
        except SerializerError as e:
            # Report malformed record, discard results, continue polling
            print("Message deserialization failed {}".format(e))
            pass

        print()

    # Leave group and commit final offsets
    consumer.close()