from .settings import redis_conn, labels, eth_deposit, opensea_buy, ens_claim, sos_claim, gasdao_claim, loot_claim, \
    polynet_address, cream_ctokens
from retry import retry

addr_len = 34


def key_gas_sum(address):
    return b"gs" + bytes.fromhex(address[2:addr_len])


def key_gas_max(address):
    return b"gm" + bytes.fromhex(address[2:addr_len])


def key_tx_count(address):
    return b"tx" + bytes.fromhex(address[2:addr_len])


def key_tx_success_count(address):
    return b"ts" + bytes.fromhex(address[2:addr_len])


def key_tx_fail_count(address):
    return b"tf" + bytes.fromhex(address[2:addr_len])


def key_tx_projects(address):
    return b"pj" + bytes.fromhex(address[2:addr_len])


def key_tx_projects_num(address):
    return b"pn" + bytes.fromhex(address[2:addr_len])


def key_tx_eth_deposit(address):
    return b"ed" + bytes.fromhex(address[2:addr_len])


def key_tx_opensea_buy(address):
    return b"ob" + bytes.fromhex(address[2:addr_len])


def key_tx_ens_claim(address):
    return b"en" + bytes.fromhex(address[2:addr_len])


def key_tx_sos_claim(address):
    return b"so" + bytes.fromhex(address[2:addr_len])


def key_tx_gasdao_claim(address):
    return b"gd" + bytes.fromhex(address[2:addr_len])


def key_tx_loot_claim(address):
    return b"lo" + bytes.fromhex(address[2:addr_len])


def key_deploy_contract(address):
    return b"dc" + bytes.fromhex(address[2:addr_len])


# hack event
def key_polynet_victim(address):
    return b"pl" + bytes.fromhex(address[2:addr_len])


def key_cream_victim(address):
    return b"cr" + bytes.fromhex(address[2:addr_len])


def key_address_set():
    return "addset"


@retry(tries=5)
def static_gas_sum(_from, _to, tx, _, p):
    gas = tx['gasUsed'] * int(tx['effectiveGasPrice'], 16) / 1e18
    key = key_gas_sum(_from)
    old_gas = float(redis_conn.get(key) or 0)
    p.set(key, old_gas + gas)


@retry(tries=5)
def static_gas_max(_from, _to, tx, _, p):
    gas = tx['gasUsed'] * int(tx['effectiveGasPrice'], 16) / 1e18
    key = key_gas_max(_from)
    old_gas = float(redis_conn.get(key) or 0)
    if gas > old_gas:
        p.set(key, gas)


@retry(tries=5)
def static_tx_count(_from, _to, tx, _, p):
    key = key_tx_count(_from)
    p.incr(key)


@retry(tries=5)
def static_tx_success_count(_from, _to, tx, _, p):
    key = key_tx_success_count(_from)
    p.incr(key)


@retry(tries=5)
def static_tx_fail_count(_from, _to, tx, _, p):
    key = key_tx_fail_count(_from)
    p.incr(key)


@retry(tries=5)
def static_tx_project(_from, _to, tx, _, p):
    if _to not in labels:
        return
    key = key_tx_projects(_from)
    p.sadd(key, _to)


@retry(tries=5)
def static_tx_project_num(_from, _to, tx, _, p):
    if _to not in labels:
        return
    key = key_tx_projects_num(_from)
    p.incr(key)


@retry(tries=5)
def static_eth_deposit(_from, _to, tx, _, p):
    if _to != eth_deposit:
        return
    key = key_tx_eth_deposit(_from)
    p.incr(key)


@retry(tries=5)
def static_opensea_buy(_from, _to, tx, _, p):
    if _to != opensea_buy:
        return
    key = key_tx_opensea_buy(_from)
    p.incr(key)


@retry(tries=5)
def static_ens_claim(_from, _to, tx, _, p):
    if _to != ens_claim:
        return
    if tx['inputData'][:10] != '0x76122903':
        return
    amount = tx['inputData'][10:74]
    amount = int(f'0x{amount}', 16) / 1e18
    key = key_tx_ens_claim(_from)
    p.set(key, amount)


@retry(tries=5)
def static_sos_claim(_from, _to, tx, _, p):
    if _to != sos_claim:
        return
    if tx['inputData'][:10] != '0xabf2ebd8':
        return
    # 真实amount舍弃了前两位 uint(256) --> uint(248)
    amount = tx['inputData'][12:74]
    amount = int(f'0x{amount}', 16) / 1e18
    key = key_tx_sos_claim(_from)
    p.set(key, amount)


@retry(tries=5)
def static_gasdao_claim(_from, _to, tx, _, p):
    if _to != gasdao_claim:
        return
    if tx['inputData'][:10] != '0x9a114cb2':
        return
    amount = tx['inputData'][10:74]
    amount = int(f'0x{amount}', 16) / 1e18
    print(amount)
    key = key_tx_gasdao_claim(_from)
    p.set(key, amount)


@retry(tries=5)
def static_loot_claim(_from, _to, tx, _, p):
    if _to != loot_claim:
        return
    if tx['inputData'][:10] != '0x379607f5':
        return
    key = key_tx_loot_claim(_from)
    p.incr(key)


@retry(tries=5)
def static_deploy_contract(_from, _to, tx, _, p):
    if _to is not None:
        return
    key = key_deploy_contract(_from)
    p.incr(key)


@retry(tries=5)
def static_polynet_victim(_from, _to, tx, block, p):
    if _to != polynet_address:
        return
    if block['number'] > 12996843:
        return
    if tx['inputData'][:10] != '0x84a6d055':
        return
    key = key_polynet_victim(_from)
    p.incr(key)


@retry(tries=5)
def static_cream_victim(_from, _to, tx, block, p):
    if _to not in cream_ctokens:
        return
    if block['number'] > 13499812:
        return
    if tx['inputData'][:10] != '0xa0712d68':
        return
    key = key_cream_victim(_from)
    p.incr(key)


@retry(tries=5)
def static_address(_from, _to, tx, _, p):
    key = key_address_set()
    p.sadd(key, _from)
