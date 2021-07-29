import ray
import ray.autoscaler.sdk

import json
import os
from time import sleep, perf_counter
from tqdm import tqdm, trange

TEST_NUM_NODES = 1000
MAX_ACTORS_IN_CLUSTER = 20000


def num_alive_nodes():
    n = 0
    for node in ray.nodes():
        if node["Alive"]:
            n += 1
    return n


def scale_to(target):
    while num_alive_nodes() != target:
        ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
        print(f"Current # nodes: {num_alive_nodes()}, target: {target}")
        print("Waiting ...")
        sleep(5)


def test_max_actors():
    # TODO (Alex): Dynamically set this based on number of cores
    cpus_per_actor = 0.24

    @ray.remote(num_cpus=cpus_per_actor)
    class Actor:
        def foo(self):
            pass

    actors = [
        Actor.options(num_cpus=0.24).remote()
        for _ in trange(MAX_ACTORS_IN_CLUSTER, desc="Launching actors")
    ]

    for actor in tqdm(actors, desc="Ensuring actors have started"):
        assert ray.get(actor.foo.remote()) is None


ray.init(address="auto")

scale_to(TEST_NUM_NODES)
assert num_alive_nodes(
) == TEST_NUM_NODES, "Wrong number of nodes in cluster " + len(ray.nodes())

cluster_resources = ray.cluster_resources()

available_resources = ray.available_resources()
assert available_resources == cluster_resources, (
    str(available_resources) + " != " + str(cluster_resources))

actor_start = perf_counter()
test_max_actors()
actor_end = perf_counter()
actor_time = actor_end - actor_start

print(f"Actor time: {actor_time} ({MAX_ACTORS_IN_CLUSTER} actors)")

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "actor_time": actor_time,
        "num_actors": MAX_ACTORS_IN_CLUSTER,
        "success": "1"
    }
    json.dump(results, out_file)
