import ray
import argparse

import random
import string
import logging
parser = argparse.ArgumentParser(prog="Stress Test - Logging")
parser.add_argument(
    "--total-num-tasks",
    type=int,
    help="Total number of tasks sending logs",
    required=True)
parser.add_argument(
    "--log-lines-per-task",
    type=int,
    help="Total lines of logs to be sent",
    required=True)

parser.add_argument(
    "--log-line-size", type=int, help="Size of a log", required=True)

args = parser.parse_args()
ray.init(address="auto")

logger = logging.getLogger(__name__)


@ray.remote
def gen_logs(log_size, log_num):

    letters = string.ascii_uppercase + string.digits

    for _ in range(log_num):
        log_line = "".join(random.choice(letters) for _ in range(log_size))
        logger.error(log_line)


ray.get([
    gen_logs.remote(args.log_line_size, args.log_lines_per_task)
    for _ in range(args.total_num_tasks)
])
