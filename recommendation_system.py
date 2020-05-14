import json
from itertools import combinations

import pandas as pd
import sqlalchemy

with open("flaskapp/local_config.json", "r") as config_file:
    config = json.load(config_file)


def get_sql_alchemy_connection():
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://{}:{}@{}/dbds_project_1".format(
            config["rds_username"], config["rds_password"], config["rds_host"]))
    return engine.connect()


def populate_freq_table(connection):
    order_ids = pd.read_sql("select distinct(order_id) from order_products", connection)
    order_products = pd.read_sql("SELECT order_id, product_id from order_products", connection)
    freq = {}

    # Increasing the frequency for all pairs of products
    for order_id in order_ids["order_id"]:
        product_ids = order_products.loc[order_products["order_id"] == order_id]["product_id"].tolist()
        for p1, p2 in combinations(product_ids, 2):
            p1, p2 = sorted([p1, p2])
            if (p1, p2) not in freq.keys():
                freq[(p1, p2)] = [p1, p2, 0]
            freq[(p1, p2)][2] += 1
    frequency_df = pd.DataFrame(list(freq.values()), columns=["p1", "p2", "freq"])
    frequency_df.to_sql("MarketBasket", conn)


if __name__ == '__main__':
    conn = get_sql_alchemy_connection()
    # get_all_orders(conn)
    # conn.execute("DROP TABLE MarketBasket")
    # conn.execute(
    #     "CREATE TABLE IF NOT EXISTS MarketBasket (product_1 varchar(255), product_2 varchar(255), freq int,
    #     CONSTRAINT MBKey PRIMARY KEY (product_1, product_2));")
    # populate_freq_table()
    res = conn.execute("select count(*) from MarketBasket")
    for row in res:
        print(row)

    recommendation = conn.execute("SELECT * FROM MarketBasket WHERE p1=39612 or p2=39612 ORDER BY freq DESC")
    for row in recommendation:
        print(row)
        break
