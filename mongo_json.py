import json

import sqlalchemy

with open("flaskapp/local_config.json", "r") as config_file:
    config = json.load(config_file)


def get_sql_alchemy_connection():
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://{}:{}@{}/dbds_project_1".format(
            config["rds_username"], config["rds_password"], config["rds_host"]))
    return engine.connect()


if __name__ == '__main__':
    conn = get_sql_alchemy_connection()
    files = ["aisles.csv", "products.csv", "orders.csv", "order_products.csv", "departments.csv"]
    table_names = ["aisles", "products", "orders", "order_products", "departments"]
    query = """SELECT
     op.product_id, 
     op.add_to_cart_order, 
     op.reordered, 
     p.product_name, 
     p.aisle_id, 
     p.department_id
     FROM order_products as op 
     LEFT JOIN products as p 
     ON op.product_id = p.product_id 
     """

    result = conn.execute(query)
    for row in result:
        print(row)
        break
    # for chunk in pd.read_sql(query, conn, chunksize=10):
    #     print(chunk)
    #     break
# for row in conn.execute(
#         "SELECT COUNT(*) FROM  orders as o LEFT JOIN order_products op ON op.order_id = o.order_id LEFT JOIN products as"
#         " p ON op.product_id = p.product_id LEFT JOIN aisles as a ON p.aisle_id = a.aisle_id LEFT JOIN departments as d"
#         " ON p.department_id = d.department_id ORDER  BY o.order_id"):
#     print(row)
