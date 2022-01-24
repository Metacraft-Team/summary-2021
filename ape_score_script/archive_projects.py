from peewee import *
from settings import redis_conn, db_password, db_port, db_host, db_user, db_schema, THREAD, badger_victim, anubis_victim
import logging
from indicator import *
from retry import retry
import multiprocessing
from archive import db, Summary


@retry(tries=5)
def get_batch_address(num):
    res = redis_conn.spop(key_address_set(), num)
    if not res:
        return []
    adds = []
    for a in res:
        adds.append(a.decode('utf-8'))
    return adds


@retry(tries=5)
def get_float_data(address, key_func):
    return float(redis_conn.get(key_func(address)) or 0)


@retry(tries=5)
def get_int_data(address, key_func):
    return int(redis_conn.get(key_func(address)) or 0)


@retry(tries=5)
def get_address_data(address_list):
    res = []
    for address in address_list:
        projects = redis_conn.smembers(key_tx_projects(address))
        projects = [p.decode('utf-8') for p in projects]
        one = {
            'address': address,
            'projects': ','.join(projects),
            'project_num': len(projects),
            'badger_victim': 0,
            'anubis_victim': 0
        }
        if address in badger_victim:
            one['badger_victim'] = 1
        if address in anubis_victim:
            one['anubis_victim'] = 1
        res.append(one)
    return res


@retry(tries=5)
def save_one(one):
    address = one['address']
    to_update = {
        'projects': one['projects'],
        'project_num': one['project_num'],
        'badger_victim': one['badger_victim'],
        'anubis_victim': one['anubis_victim'],
    }
    num = Summary.update(to_update).where(Summary.address == address).execute()
    if num == 0:
        logging.error(f'update {address} fail')


def save():
    step = 1000
    adds = get_batch_address(step)
    while adds:
        print(len(adds))
        data = get_address_data(adds)
        for one in data:
            try:
                save_one(one)
            except Exception as e:
                print(e)
        adds = get_batch_address(step)


if __name__ == "__main__":
    thread_list = []
    i = 0
    while i < THREAD:
        t = multiprocessing.Process(target=save, args=())
        thread_list.append(t)
        i += 1
    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()

    print("done.")
    logging.info("done")
