import json

import pandas as pd
import smart_open
import sqlalchemy

with open("flaskapp/local_config.json", "r") as config_file:
    config = json.load(config_file)


def get_sql_alchemy_connection():
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://{}:{}@{}/dbds_project_1".format(
            config["rds_username"], config["rds_password"], config["rds_host"]))
    return engine.connect()


def load_instacart_to_sql(file_names, connection):
    for file_name in file_names:
        access_url = "s3://{}:{}@dbds-project/instacart/{}".format(config["aws_access_key_id"],
                                                                   config["aws_secret_access_key"], file_name)
        with smart_open.open(access_url, "r") as f:
            df = pd.read_csv(f)

        df.to_sql(file_name[:-4], connection, if_exists="replace", index=False)


if __name__ == '__main__':
    conn = get_sql_alchemy_connection()
    files = ["aisles.csv", "products.csv", "orders.csv", "order_products.csv", "departments.csv"]
    load_instacart_to_sql(files, conn)
    for file in files:
        table_name = file[:-4]
        for row in conn.execute("select count(*) from {}".format(table_name)):
            print(row)
