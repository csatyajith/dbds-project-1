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
o.order_id, o.user_id, o.order_number, O.order_dow, O.order_hour_of_day, O.days_since_prior_order
"
", Op.product_id, Op.add_to_cart_order, Op.reordered, p.product_name, p.aisle_id, p.department_id, a.aisle"
", d.department
