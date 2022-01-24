import time
import multiprocessing

from web3 import Web3
from indicator import *
from .settings import logging, graphqls, start_block, end_block, step, THREAD
from sgqlc.endpoint.http import HTTPEndpoint

static_func = [
    static_gas_sum,
    static_gas_max,
    static_tx_count,
    static_address
]

static_func_success = [
    static_tx_success_count,
    static_tx_project,
    static_eth_deposit,
    static_opensea_buy,
    static_ens_claim,
    static_sos_claim,
    static_gasdao_claim,
    static_loot_claim,
    static_deploy_contract,
    static_polynet_victim,
    static_cream_victim
]


def get_current_block():
    res = redis_conn.incr("current_block", step)
    if int(res) < 1000:
        redis_conn.set("current_block", start_block)
        return start_block
    return int(res)


def set_start_block(block):
    redis_conn.set("start_block", block)


@retry(tries=5)
def run_static_func(tx, block, p):
    _from = tx["from"]
    _to = tx["to"]
    if tx['status'] == 1:
        for f in static_func_success:
            f(_from, _to, tx, block, p)
    else:
        static_tx_fail_count(_from, _to, tx, block, p)

    for f in static_func:
        f(_from, _to, tx, block, p)

    p.execute()


class Spider():
    def __init__(self, url):
        self.endpoint = HTTPEndpoint(url)

    @retry(tries=5)
    def get_block(self, from_block, to_block):
        query = "query c{blocks(from:%d,to:%d){number hash timestamp transactions{hash status gasUsed effectiveGasPrice from{address} to{address} inputData}}}" % (
            from_block, to_block)
        return self.endpoint(query)

    @retry(tries=5)
    def get_tx(self, tr_hash):
        return self.w3.eth.get_transaction_receipt(tr_hash), self.w3.eth.get_transaction_receipt(tr_hash)

    def parse_block_detail(self, from_block, to_block):
        blocks = self.get_block(from_block, to_block)
        for block in blocks['data']['blocks']:
            for transaction in block["transactions"]:
                transaction['from'] = Web3.toChecksumAddress(transaction['from']['address'])
                if transaction['to'] is not None:
                    transaction['to'] = Web3.toChecksumAddress(transaction['to']['address'])

                with redis_conn.pipeline(transaction=True) as p:
                    try:
                        run_static_func(transaction, block, p)
                    except Exception as e:
                        logging.error(f"{transaction['hash']} {e}")

    # def parse_transaction(self, tr_hash):
    #     tr, trp = self.get_tx(tr_hash)
    #
    #     for f in static_func:
    #         try:
    #             f(tr, trp)
    #         except Exception as e:
    #             logging.error(f"{tr_hash} {f.__name__} {e}")

    def product(self):
        start = 0
        while start <= end_block:
            start = get_current_block()
            print("get_current_block", start)
            end = start + step - 1
            if end > end_block:
                end = end_block
            self.parse_block_detail(start, end)
            logging.info(f"success handle block {start} to {end}")


if __name__ == "__main__":
    thread_list = []
    i = 0
    if not redis_conn.get("current_block"):
        redis_conn.set("current_block", start_block - step)

    while i < THREAD:
        url = graphqls[i % len(graphqls)]
        t = multiprocessing.Process(target=Spider(url).product, args=())
        thread_list.append(t)
        i += 1
    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()
