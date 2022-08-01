from QueryBuilderClass import KsqlQueryConstructor


with open("subscription4.json", "r") as f:
    json_data = f.read()
query_instance = KsqlQueryConstructor(json_data)
# print(json_data)
q1, q2, q3 = query_instance.construct_ksql_queries()
print(q1)
print("\n")
print(q2)
print("\n")
print(q3)