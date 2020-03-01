import json

import sqlalchemy
from flask import Flask

app = Flask(__name__)

with open("local_config.json", "r") as config_file:
    config = json.load(config_file)


def get_sql_alchemy_connection():
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://{}:{}@{}/dbds_project_1".format(
            config["rds_username"], config["rds_password"], config["rds_host"]))
    return engine.connect()


@app.route('/')
def my_form():
    return "Hello World from Flask"


#
# @app.route('/', methods=['POST'])
# def my_form_post():
#     text = request.form['text']
#     processed_text = text.upper()
#     return processed_text


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)
