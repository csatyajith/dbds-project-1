import json

import psycopg2
import sqlalchemy
from flask import Flask, request, render_template
from psycopg2 import Error

app = Flask(__name__)

with open("local_config.json", "r") as config_file:
    config = json.load(config_file)


class QueryHandler:
    def __init__(self):
        self.rds_connection = self.get_rds_connection()
        self.redshift_connection = self.get_redshift_connection()

    @staticmethod
    def get_redshift_connection():
        return psycopg2.connect(dbname='dev', host=config['redshift_host'],
                                port="5439", user=config["redshift_username"],
                                password=config["redshift_password"])

    @staticmethod
    def get_rds_connection():
        engine = sqlalchemy.create_engine(
            "mysql+pymysql://{}:{}@{}/dbds_project_1".format(
                config["rds_username"], config["rds_password"], config["rds_host"]))
        return engine.connect()

    def execute_redshift_query(self, query):
        cur = self.redshift_connection.cursor()
        cur.execute(query)
        return cur.fetchall()

    def execute_rds_query(self, query):
        return self.rds_connection.execute(query)


@app.route('/')
def my_form():
    return render_template('frontend_insta.html')


@app.route('/', methods=['POST'])
def my_form_post():
    text = request.form['text_box']
    query_lang = None
    if "Query_lang" in request.form:
        query_lang = request.form["Query_lang"]
    if not query_lang:
        return "Please select a query language"

    try:
        if query_lang == "MySQL":
            results = list()
            for row in query_handler.execute_rds_query(text):
                results.append(dict(row))
            return json.dumps(results)
        elif query_lang == "Redshift":
            results = list()
            for row in query_handler.execute_redshift_query(text):
                results.append(row)
            return str(results)
    except (SyntaxError, Error):
        return "Query is invalid. Please enter a valid query"


if __name__ == '__main__':
    query_handler = QueryHandler()
    app.run(host="0.0.0.0", port=80)
