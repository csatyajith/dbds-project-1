import json

import psycopg2
import sqlalchemy
from flask import Flask, request, render_template
from psycopg2 import Error
from sqlalchemy.exc import ProgrammingError

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
        return cur

    def execute_rds_query(self, query):
        return self.rds_connection.execute(query)


@app.route('/')
def my_form():
    return render_template('frontend_insta.html')


@app.route('/', methods=['POST'])
def my_form_post():
    query_lang = None
    if "Query_lang" in request.form:
        query_lang = request.form["Query_lang"]
    if not query_lang:
        return render_template("frontend_insta.html", other_text="Please select a database")

    text = request.form['text_box']
    if text is None or len(text) == 0:
        return render_template("frontend_insta.html", other_text="Please type a query")
    try:
        if query_lang == "MySQL":
            results = list()
            incomplete_data = False
            for i, row in enumerate(query_handler.execute_rds_query(text)):
                results.append(dict(row))
                if i >= 5000:
                    incomplete_data = True
                    break
            if incomplete_data:
                return render_template("frontend_insta.html", columns=results[0].keys(), items=results,
                                       other_text="Query result too large. Showing first 5000 results")
            return render_template("frontend_insta.html", columns=results[0].keys(), items=results)
        elif query_lang == "Redshift":
            cur = query_handler.execute_redshift_query(text)
            field_names = [i[0] for i in cur.description]
            result = []
            incomplete_data = False
            for n, res in enumerate(cur):
                if n >= 1000:
                    incomplete_data = True
                    break
                entry = {}
                for i, field_name in enumerate(field_names):
                    entry[field_name] = res[i]
                result.append(entry)

            if incomplete_data:
                return render_template("frontend_insta.html", columns=result[0].keys(), items=result,
                                       other_text="Query result too large. Showing first 5000 results")
            return render_template("frontend_insta.html", columns=result[0].keys(), items=result)
    except (ProgrammingError, Error):
        return render_template("frontend_insta.html", other_text="Query is invalid. Please enter a valid query")


if __name__ == '__main__':
    query_handler = QueryHandler()
    app.run(host="0.0.0.0", port=80)
