import json

import psycopg2

with open("local_config.json", "r") as config_file:
    config = json.load(config_file)

# engine = sqlalchemy.create_engine("redshift+psycopg2://{}:{}@{}".format(
#     config["redshift_username"], config["redshift_password"], config["redshift_host"]))
# connection = engine.connect()

connection = psycopg2.connect(dbname='dev', host=config['redshift_host'],
                              port="5439", user=config["redshift_username"], password=config["redshift_password"])

cur = connection.cursor()
cur.execute("select * from aisles;")
print(cur.fetchall())
cur.close()
connection.close()
