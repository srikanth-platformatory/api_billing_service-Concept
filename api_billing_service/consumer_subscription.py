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
    consumer_conf['group.id'] = 'python_example_group_345'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])


    query_dict_list = []

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
                
                # print(record_key)
                # print()
                # pprint(data, indent=3)

                subscription_id = data['subscription_id'].strip()
                subscription_for = data['subscription_for'].strip()
                subscription_from = data['subscription_duration']['from'].strip()
                subscription_to = data['subscription_duration']['to'].strip()
                field_reference = data['subscription_plan']['metering_units']['field_ref'].replace("response.headers.", '').strip()
                aggregation_type = data['subscription_plan']['metering_units']['aggregated_as'].strip().upper()
                subscription_id_source = data['subscription_plan']['subscription_id_source'].replace("response.headers.", '').strip()

                print(subscription_id, subscription_for, subscription_from, subscription_to, field_reference, aggregation_type, subscription_id_source)

                # query = f"""select subscription_id, {aggregation_type}({field_reference}), consumer_id, consumer_created_at, consumer_user from api_logs_avro where now() between '{subscription_from}' AND '{subscription_to}' WINDOW TUMBLING (SIZE 5 MINUTES) GROUP BY subscription_id ,consumer_id ,consumer_user ;"""
                # changed query from ^ to v

                query = f"""SELECT  REQUEST->HEADERS->SUBSCRIPTION_ID as SUBSCRIPTION_ID, consumer->id as consumer_id, consumer->username as user, {aggregation_type}(CAST(REQUEST->HEADERS->{field_reference} AS INTEGER) ) VALUE, WINDOWSTART START_TIME, WINDOWEND END_TIME FROM S1 WINDOW TUMBLING ( SIZE 30 SECONDS ) WHERE (RESPONSE->STATUS < 300) GROUP BY REQUEST->HEADERS->SUBSCRIPTION_ID, consumer->id, consumer->username EMIT CHANGES;"""
                
                query_dict_item = {'subscription_id': subscription_id, 'query': query}
                query_dict_list.append(query_dict_item)

                # query_string_list.append(query)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

    # print(query_dict, '\n', type(query_dict))
    with open("queries.ksql", 'w') as f:
        # f.write('\n'.join(query_dict))
        json.dump(query_dict, f)




# ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-events
# ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-payload
#  ./consumer.py -f /home/dellvostro/projects/librdkafka.config -t api-specs