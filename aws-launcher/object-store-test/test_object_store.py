import numpy as np

import ray
import ray.autoscaler.sdk

import json
import os
from time import perf_counter
from tqdm import tqdm

NUM_NODES = 20
OBJECT_SIZE = 2**30 * 10


def test_object_broadcast():
    @ray.remote(num_cpus=1, resources={"node": 1})
    class Actor:
        def foo(self):
            pass

        def sum(self, arr):
            return len(arr)

    actors = [Actor.remote() for _ in range(NUM_NODES)]

    arr = np.ones(OBJECT_SIZE, dtype=np.uint8)
    ref = ray.put(arr)

    for actor in tqdm(actors, desc="Ensure all actors have started."):
        ray.get(actor.foo.remote())

    start = perf_counter()
    result_refs = []
    for actor in tqdm(actors, desc="Broadcasting objects"):
        result_refs.append(actor.sum.remote(ref))

    results = ray.get(result_refs)
    end = perf_counter()
    print(f"Broadcast time: {end - start} ({OBJECT_SIZE} B x {NUM_NODES} nodes)")

ray.init(address="auto")
test_object_broadcast()


if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "broadcast_time": end - start,
        "object_size": OBJECT_SIZE,
        "num_nodes": NUM_NODES,
        "success": "1",
    }
    perf_metric_name = f"time_to_broadcast_{OBJECT_SIZE}_bytes_to_{NUM_NODES}_nodes"
    results["perf_metrics"] = [
        {
            "perf_metric_name": perf_metric_name,
            "perf_metric_value": end - start,
            "perf_metric_type": "LATENCY",
        }
    ]
    json.dump(results, out_file)
