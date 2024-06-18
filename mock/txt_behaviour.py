# Registro de comportamento txt (logs)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json
import os
import time
import mock_utils as MOCK
import random

# Generate txt log data
def generate_txt_log_data(log_type: str, quantity: int = 1000):
    if log_type == "behaviour":
        return [MOCK.generateLogUserBehavior() for _ in range(quantity)]
    if log_type == "audit":
        return [MOCK.generateLogAudit() for _ in range(quantity)]
    if log_type == "fail":
        return [MOCK.generateLogFailureNotification() for _ in range(quantity)]
    if log_type == "debug":
        return [MOCK.generateLogDebug() for _ in range(quantity)]
    return "Invalid log type"

N = 100
data_path = "data"
type_list = ["behaviour", "behaviour", "behaviour",
             "behaviour", "behaviour", "behaviour",
             "audit", "fail", "debug"]

for type in type_list:
    os.makedirs(f"{data_path}/{type}", exist_ok=True)
    for i in range(N):
        with open(f"{data_path}/{type}/log_{i}.txt", "w") as f:
            random_num = random.randint(200, 1000)
            log_data = generate_txt_log_data(type, quantity=random_num)
            for log in log_data:
                f.write(f"{log}\n")