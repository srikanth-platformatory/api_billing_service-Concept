import json
import re

class KsqlQueryConstructor:

    def __init__(self, subscription_json, **kwargs):
        if isinstance(subscription_json, str):
            self.plan = json.loads(subscription_json)
        else:
            self.plan = subscription_json
            
        sub_match = re.fullmatch("[a-zA-Z0-9-/_/]+$", str(self.plan["subscription_id"]))

        if (len(self.plan["subscription_id"]) == 0) or (not sub_match):
            raise Exception("Invalid value for subscription_id. subscription_id cannot be empty or contain special character other than '-', '_'")
        
        self.subscription_id = str(self.plan["subscription_id"]).replace("-", "_")
        self.service_name = self.plan["service_name"]
        self.start_date = self.plan["subscription_duration"]["from"]
        self.end_date = self.plan["subscription_duration"]["to"]
        st_match = re.fullmatch("^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$", self.start_date)
        en_match = re.fullmatch("^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$", self.end_date)
        
        if (not st_match) or (not en_match):
            raise Exception("Invalid Date Format. Subscription date should be of the format YYYY-MM-DD")
        
        self.plan_id = self.plan["subscription_plan"]["id"]
        self.plan_name = self.plan["subscription_plan"]["name"]
        
    def construct_field_from_path(self, field_ref):
        path_split = field_ref.split(".")
        if (path_split[0] in ["request", "response"]) and (path_split[1] in ["headers", "header", "body"]):
            field_path = "EXTRACTJSONFIELD(%s->%s, '$.%s')" % (path_split[0], path_split[1], ".".join(path_split[2:]))
        else:
            field_path = "->".join(path_split)
        return field_path
    
    def construct_filters(self):
        criterias = self.plan["subscription_plan"]["bill_criteria"]
        
        if len(criterias) == 0:
            raise Exception("Minimum of 1 filter has to be present")
        
        filters_list = []
        is_subscription_id = False
        for criteria in criterias:
            if "subscription_id" in str(criteria["field_ref"]):
                is_subscription_id = True
            criteria_field_ref = criteria["field_ref"]
            criteria_field_path = self.construct_field_from_path(criteria_field_ref)
            if criteria["type"] in ["STRING", "VARCHAR"]:
                criteria_value = "\'%s\'" % criteria["value"]
            else:
                criteria_value = criteria["value"]
            
            if len(criteria["operator"]) == 0:
                raise Exception("Invalid Operator value. Opearator field cannot be empty")

            if criteria["operator"] not in ['<', '>', '=', '!=', '<=', '=>', '<>']:
                raise Exception("Invalid Operator value. %s is not a valid operator" % criteria["operator"])
            
            filter_str = "(" + " ".join([criteria_field_path, criteria["operator"], criteria_value]) + ")"
            filters_list.append(filter_str)
        
        if not is_subscription_id:
            raise Exception("subscription_id should be present in filters") 
        
        return "WHERE " + " AND ".join(filters_list)

    def construct_selectors(self):
        selector_str = "CREATE TABLE %s_%s_WINDOWED_TABLE WITH (KAFKA_TOPIC='%s_%s_windowed', KEY_FORMAT='JSON', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=3) AS SELECT WINDOWSTART start_time, WINDOWEND end_time, " % (self.service_name, self.subscription_id, self.service_name, self.subscription_id)
        group_str = "GROUP BY "
        selector_fields = self.plan["subscription_plan"]["selector_fields"]
        # sub_id_source = self.plan["subscription_plan"]["subscription_id_source"]
        # sub_id_field_path = self.construct_field_from_path(sub_id_source)
        # sub_id_str = " ".join([sub_id_field_path, "AS", "subscription_id"])
        selectors_path = []
        selectors_name = []
        selectors = []
        is_subscription_id = False
        for selector in selector_fields:
            if "subscription_id" in str(selector["field_ref"]):
                is_subscription_id = True
            selector_path = self.construct_field_from_path(selector["field_ref"])
            selectors_path.append(selector_path)
            selectors.append(selector_path + " " + str(selector["name"]))
            selectors_name.append(str(selector["name"]) + " " + selector["type"])
        
        if not is_subscription_id:
            raise Exception("subscription_id should be present in selector_fields") 
        
        agg_selector_str = self.construct_aggregators()
        selector_str = selector_str + ", ".join(selectors) + agg_selector_str
        group_str = group_str + ", ".join(selectors_path)
        stream_str = "start_time BIGINT, end_time BIGINT, " + ", ".join(selectors_name) + ", aggregated_units BIGINT"
        return selector_str, group_str, stream_str

    def construct_aggregators(self):
        aggregator_str = ", "
        metering_units = self.plan["subscription_plan"]["metering_units"]
        if metering_units["type"] == "custom":
            metering_field_ref = metering_units["field_ref"]
            metering_field_path = self.construct_field_from_path(metering_field_ref)
            aggregator_str = aggregator_str + "%s(CAST(%s AS INT)) aggregated_units" % (metering_units["aggregated_as"], metering_field_path)    
        else:
            pass
        return aggregator_str

    def construct_ksql_queries(self):
        select, group, stream = self.construct_selectors()
        filter = self.construct_filters()
        agg_query = " ".join([select, "FROM DEMO_API_LOG_AVRO WINDOW TUMBLING (SIZE 30 SECONDS)", filter, group]) + ";"
        stream_query = "CREATE STREAM %s_%s_WINDOWED_STREAM (" % (self.service_name, self.subscription_id) + stream + ") WITH (KAFKA_TOPIC='%s_%s_windowed', KEY_FORMAT='JSON', VALUE_FORMAT='JSON');" % (self.service_name, self.subscription_id)
        output_query = "INSERT INTO %s_AGGREGATED_OUTPUT SELECT * FROM %s_%s_WINDOWED_STREAM;" % (self.service_name, self.service_name, self.subscription_id)
        return agg_query, stream_query, output_query