import json
import math
import statistics
from collections import Counter

import numpy as np
import pymongo


def get_mongo_client():
    return pymongo.MongoClient("Insert server link here")


def get_count_for_column(collection, column_name, column_value):
    query = {column_name: column_value}
    count = collection.count_documents(query)

    print("Count is: {}".format(count))


def get_count_percent_for_column(collection, column_name, column_value):
    query = {column_name: column_value}
    count = collection.count_documents(query)
    total_count = collection.count_documents({})
    print("Count percent is: {}".format((count / total_count) * 100))


def multimode(data):
    res = []
    test_list1 = Counter(data)
    temp = test_list1.most_common(1)[0][1]
    for ele in data:
        if data.count(ele) == temp:
            res.append(ele)
    return list(set(res))


def generate_stats(collection, column_name):
    print("Generating statistics for column:", column_name)
    query = [
        {
            "$group": {
                "_id": None,
                "Average": {
                    "$avg": "${}".format(column_name)
                },
                "Minimum": {
                    "$min": "${}".format(column_name)
                },
                "Maximum": {
                    "$max": "${}".format(column_name)
                }
            }
        }
    ]
    result = collection.aggregate(query)
    basic_stats = result.next()
    for key in basic_stats:
        if key != "_id":
            print("{}: {}".format(key, basic_stats[key]))

    all_docs = collection.find({}, {column_name: 1})
    column_values_list = []
    for doc in all_docs:
        column_values_list.append(doc[column_name])
    print("Median: {}".format(statistics.median(column_values_list)))
    print("Mode: {}".format(multimode(column_values_list)))
    print("Standard Deviation: {}".format(statistics.stdev(column_values_list)))
    print("Variance: {}".format(statistics.variance(column_values_list)))
    print("Range: {}".format(basic_stats["Maximum"] - basic_stats["Minimum"]))
    print("Coefficient of variation: {}".format(statistics.stdev(column_values_list) / basic_stats["Average"]))


def insert_records(collection):
    with open("influenza/combined_data.json") as f:
        data = json.load(f)

    data = data[1:]
    for item in data:
        collection.insert_one(item)


def chi_square(collection, column_1, column_2):
    all_docs = collection.find({}, {column_1: 1, column_2: 1})
    column_1_values = []
    column_2_values = []
    pairs = []
    for doc in all_docs:
        column_1_values.append(doc[column_1])
        column_2_values.append(doc[column_2])
        pairs.append((doc[column_1], doc[column_2]))
    unique_col_1 = list(set(column_1_values))
    unique_col_2 = list(set(column_2_values))
    chi_square_list = []
    for c1 in unique_col_1:
        for c2 in unique_col_2:
            e = (column_1_values.count(c1) * column_2_values.count(c2)) / len(column_1_values)
            n = pairs.count((c1, c2))
            chi_square_list.append(((n - e) ** 2) / e)
    print("Chi square for {}, {} is: {}".format(column_1, column_2, sum(chi_square_list)))


def linear_correlation(collection, column_1, column_2):
    all_docs = collection.find({}, {column_1: 1, column_2: 1})
    column_1_values = []
    column_2_values = []
    for doc in all_docs:
        column_1_values.append(doc[column_1])
        column_2_values.append(doc[column_2])

    var_1 = statistics.variance(column_1_values)
    var_2 = statistics.variance(column_2_values)
    covar = np.cov(column_1_values, column_2_values)

    print(
        "Linear Correlation between {}, {} is: {}".format(column_1, column_2, covar[0][1] / math.sqrt((var_1 * var_2))))


def z_value(collection, categorical_col, value_1, value_2, numerical_col):
    all_docs = collection.find({}, {categorical_col: 1, numerical_col: 1})
    set_1 = []
    set_2 = []
    for doc in all_docs:
        if doc[categorical_col] == value_1:
            set_1.append(doc[numerical_col])
        if doc[categorical_col] == value_2:
            set_2.append(doc[numerical_col])

    mean_1 = statistics.mean(set_1)
    mean_2 = statistics.mean(set_2)
    var_1 = statistics.variance(set_1)
    var_2 = statistics.variance(set_2)

    z = (mean_1 - mean_2) / (math.sqrt((((var_1 ** 2) / (len(set_1))) + ((var_2 ** 2) / (len(set_2))))))
    print("Z value between {}, {} is: {}".format(categorical_col, numerical_col, z))


if __name__ == '__main__':
    client = get_mongo_client()
    db = client["DBDSAssignment"]
    wide_collection = db["WideDataset"]
    get_count_for_column(wide_collection, "target", "uninfected")
    get_count_percent_for_column(wide_collection, "target", "uninfected")
    generate_stats(wide_collection, "11715101_s_at")
    chi_square(wide_collection, "target", "time point")
    linear_correlation(wide_collection, "11715100_at", "11715101_s_at")
    z_value(wide_collection, "time point", "48hrs", "baseline", "11715100_at")
