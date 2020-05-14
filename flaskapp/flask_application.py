import json
import time

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


@app.route('/recommendations')
def my_form_2():
    return render_template("my-form.html")


@app.route("/recommendations", methods=['POST'])
def my_form_2_post():
    try:
        print("1")
        result = query_handler.execute_rds_query(
            "SELECT * FROM MarketBasket WHERE p1={} or p2={} ORDER BY freq DESC".format(request.form["text_box"],
                                                                                        request.form["text_box"]))
        print("2")
        for r in result:
            product_1_result = query_handler.execute_rds_query(
                "select * from products where product_id={}".format(r[1]))

            product_2_result = query_handler.execute_rds_query(
                "select * from products where product_id={}".format(r[2]))

            return render_template("my-form.html",
                                   recommended_product="The recommendations are: {}, {}".format(next(product_1_result),
                                                                                                next(product_2_result)),
                                   freq="The number of times they occur together is: {}".format(r[3]))
    except (ProgrammingError, Error):
        return render_template("my-form.html", freq="Query is invalid. Please enter a valid query")


@app.route('/', methods=['POST'])
def my_form_post():
    time1 = time.time()
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
            time2 = time.time()
            if incomplete_data:
                return render_template("frontend_insta.html", columns=results[0].keys(), items=results,
                                       other_text="Query result too large. Showing first 5000 results",
                                       perf_time="Time elapsed: {}".format(time2 - time1))
            return render_template("frontend_insta.html", columns=results[0].keys(), items=results,
                                   perf_time="Time elapsed: {}".format(time2 - time1))
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

            time2 = time.time()
            if incomplete_data:
                return render_template("frontend_insta.html", columns=result[0].keys(), items=result,
                                       other_text="Query result too large. Showing first 5000 results",
                                       perf_time="Time elapsed: {}".format(time2 - time1))
            return render_template("frontend_insta.html", columns=result[0].keys(), items=result,
                                   perf_time="Time elapsed: {}".format(time2 - time1))
    except (ProgrammingError, Error):
        return render_template("frontend_insta.html", other_text="Query is invalid. Please enter a valid query")


if __name__ == '__main__':
    query_handler = QueryHandler()
    app.run(host="localhost")
