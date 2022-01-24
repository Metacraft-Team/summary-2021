from peewee import *
import logging
from .indicator import *
from retry import retry
import multiprocessing

from .settings import redis_conn, db_password, db_port, db_host, db_user, db_schema, THREAD, badger_victim, \
    anubis_victim

db = MySQLDatabase(db_schema, user=db_user, password=db_password, host=db_host, port=db_port)


class Summary(Model):
    address = CharField()
    gas_all = DoubleField()
    gas_max = DoubleField()
    project_num = IntegerField()
    tx_num = IntegerField()
    tx_success_num = IntegerField()
    tx_fail_num = IntegerField()
    projects = CharField()
    eth_deposit = IntegerField()
    opensea_buy = IntegerField()
    ens_claim = DoubleField()
    sos_claim = DoubleField()
    gasdao_claim = DoubleField()
    loot_claim = IntegerField()
    contract_deploy = IntegerField()
    polynet_victim = IntegerField()
    cream_victim = IntegerField()
    badger_victim = IntegerField()
    anubis_victim = IntegerField()

    class Meta:
        database = db
        db_table = 'metacraft_summary_2'


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
            'gas_all': get_float_data(address, key_gas_sum),
            'gas_max': get_float_data(address, key_gas_max),
            'projects': ','.join(projects),
            'project_num': len(projects),
            'tx_num': get_int_data(address, key_tx_count),
            'tx_success_num': get_int_data(address, key_tx_success_count),
            'tx_fail_num': get_int_data(address, key_tx_fail_count),
            'eth_deposit': get_int_data(address, key_tx_eth_deposit),
            'opensea_buy': get_int_data(address, key_tx_opensea_buy),
            'ens_claim': get_float_data(address, key_tx_ens_claim),
            'sos_claim': get_float_data(address, key_tx_sos_claim),
            'gasdao_claim': get_float_data(address, key_tx_gasdao_claim),
            'loot_claim': get_int_data(address, key_tx_loot_claim),
            'contract_deploy': get_int_data(address, key_deploy_contract),
            'polynet_victim': get_int_data(address, key_polynet_victim),
            'cream_victim': get_int_data(address, key_cream_victim),
            'badger_victim': 0,
            'anubis_victim': 0
        }
        if address in badger_victim:
            one['badger_victim'] = 1
        if address in anubis_victim:
            one['anubis_victim'] = 1
        res.append(one)
    return res


def save():
    step = 1000
    adds = get_batch_address(step)
    while adds:
        print(len(adds))
        data = get_address_data(adds)
        try:
            with db.atomic():
                Summary.insert_many(data).execute()
                logging.info(f"success to save {len(adds)}")
        except Exception as e:
            print(e)
            logging.error(f"save {adds} err {e}")
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
