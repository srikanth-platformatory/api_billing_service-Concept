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
from QueryBuilderClass import KsqlQueryConstructor
import logging
from ksql import KSQLAPI
logging.basicConfig(level=logging.DEBUG)
from query_producer import query_producer




if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_26'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # initialize ksql client
    client = KSQLAPI("https://pksqlc-g9mm3.centralindia.azure.confluent.cloud:443", api_key="LPJPDUA4ECG5PIVD", secret="U4R+W+WhT+PUMl4qvVzf9sumzd4p7EAKziswkSQbT6UHZ/dbpgg3Z/diicXxdz30")

    subscriptions_dict_list = []

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
                
                query_instance = KsqlQueryConstructor(data)
                # print(json_data)
                q1, q2, q3 = query_instance.construct_ksql_queries()    
                
                subscription_id = query_instance.subscription_id
                service_name = query_instance.subscription_id
                start_date = query_instance.start_date
                end_date = query_instance.end_date
                plan_id = query_instance.plan_id
                plan_name = query_instance.plan_name

                query_dict_item = {'subscription_id': subscription_id, 'service_name': service_name, 'start_date': start_date, 'end_date': end_date, 'plan_id': plan_id, 'plan_name': plan_name, 'queries': [q1,q2,q3] }

                query_ids_list = []

                # call ksql to get q_id for each query
                for x in [q1, q2, q3]:
                    details_q1 = client.ksql(x)
                    print("\n Query details are : \n ", details_q1, end='\n')
                    
                    # print(details_q1, "\n\n", type(details_q1))
                    print()
                    query_id = details_q1[0]['commandStatus']['queryId']
                    print(query_id, end = '\n')
                    query_ids_list.append(query_id)
                    
                

                query_dict_item['query_ids'] = query_ids_list

                
                print(query_dict_item, end = '\n')

                

                query_producer(topic = 'demo_query_commands', key = subscription_id, msg = json.dumps(query_dict_item) )
                

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

    # # previous approach, build queries -> write to queries.ksql -> produce to cmds topic using query_producer.py
    # # print(query_dict, '\n', type(query_dict))
    # with open("queries.ksql", 'w') as f:
    #     # f.write('\n'.join(query_dict))
    #     json.dump(query_dict, f)




# ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-events
# ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-payload
#  ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-specs