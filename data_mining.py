import random

import numpy as np
from scipy.spatial import distance

from data_exploration_assignm import get_mongo_client


def get_top_ten_probes(collection, probes):
    returnable = {}
    for probe in probes:
        returnable[probe] = 1
    return collection.find({}, returnable)


def get_k_clusters(rows, probes, k):
    # Transforming the generator to a list of lists
    samples = []
    for row in rows:
        sample = []
        for probe in probes:
            sample.append(row[probe])
        samples.append(sample)

    # Choosing k random clusters
    cluster_centers = random.choices(samples, k=k)

    # Iterating through the samples and finding the closes centroid for each sample
    for sample in samples:
        smallest_distance = np.inf
        closest = 0
        for i, cluster_center in enumerate(cluster_centers):
            d = distance.euclidean(sample, cluster_center)
            if d < smallest_distance:
                smallest_distance = d
                closest = i
        # Multiplying the centroid with the length to ensure that the mean is accurate
        p = [c * len(cluster_centers) for c in cluster_centers[closest]]

        # Re-computing the centroid after placing the probe in one of the clusters
        cluster_centers[closest] = [sum(i) / len(cluster_centers[closest]) for i in zip(p, sample)]

    return cluster_centers


if __name__ == '__main__':
    client = get_mongo_client()
    db = client["DBDSAssignment"]
    wide_collection = db["WideDataset"]
    my_probes = ["11715100_at", "11715101_s_at", "11715102_x_at", "11715103_x_at", "11715104_s_at", "11715105_at",
                 "11715106_x_at", "11715107_s_at", "11715108_x_at", "11715109_at"]
    top_ten_probes = get_top_ten_probes(wide_collection, my_probes)
    clusters = get_k_clusters(top_ten_probes, my_probes, 5)
    for center in clusters:
        print([round(c, 2) for c in center])
