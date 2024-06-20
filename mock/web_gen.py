# Envio de registros de comportamento para webhook
import time
import random
import multiprocessing
import sys
import os
import user_class
import my_settings as CONFIG


def user_process(num_users, sleep_time):
    users_list = [user_class.User(i) for i in range(1, num_users + 1)]

    while True:
        user = random.choice(users_list)
        user.view_item()

        time.sleep(sleep_time)

settings = CONFIG.Configuration
num_processes = settings.NUM_PROCESSES
num_users = settings.NUM_USERS
sleep_time = settings.SLEEP_TIME

time.sleep(5) # Espera o broker subir

processes = []
for _ in range(num_processes):
    p = multiprocessing.Process(target=user_process, args=(num_users, sleep_time))
    processes.append(p)
    p.start()

for p in processes:
    p.join()