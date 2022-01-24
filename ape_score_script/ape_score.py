import multiprocessing

from peewee import *

from .archive import Summary, db
from .settings import redis_conn


def cal_score(item):
    total_score = 0
    details = []
    if not item:
        return 0, details

    # gas all
    score = 0
    title = f"spend gas {round(item.gas_all, 2)} ETH"
    if 0 < item.gas_all < 0.1:
        score = 100
        details.append({
            "score": score,
            "title": title,
            "reason": "be thrifty"
        })
    elif 0.1 <= item.gas_all < 0.3:
        score = 200
        details.append({
            "score": score,
            "title": title,
            "reason": ""
        })
    elif 0.3 <= item.gas_all < 1:
        score = 300
        details.append({
            "score": score,
            "title": title,
            "reason": ""
        })
    elif 1 <= item.gas_all < 5:
        score = 500
        details.append({
            "score": score,
            "title": title,
            "reason": ""
        })
    elif 5 <= item.gas_all < 10:
        score = 800
        details.append({
            "score": score,
            "title": title,
            "reason": ""
        })
    elif 10 <= item.gas_all < 20:
        score = 1200
        details.append({
            "score": score,
            "title": title,
            "reason": ""
        })
    elif item.gas_all >= 20:
        score = 2000
        details.append({
            "score": score,
            "title": title,
            "reason": "⚠️ warning gas leak"
        })
    total_score += score

    # project
    score = item.project_num * 45
    if score > 2000:
        score = 2000
    if score > 0:
        details.append({
            "score": score,
            "title": f"used more than {item.project_num} projects",
            "reason": ""
        })
    total_score += score

    # opensea buy
    score = item.opensea_buy * 125
    if score > 2000:
        score = 2000
    if score > 0:
        details.append({
            "score": score,
            "title": f"used opensea buy NFT {item.opensea_buy} times",
            "reason": ""
        })
    total_score += score

    # tx num
    score = item.tx_num * 0.1
    if score > 200:
        score = 200
    if score > 0:
        details.append({
            "score": score,
            "title": f"{item.tx_num} transactions sent",
            "reason": ""
        })
    total_score += score

    # hack
    if item.polynet_victim:
        score = 500
        details.append({
            "score": score,
            "title": f"poly network hacking victim",
            "reason": ""
        })
        total_score += score
    if item.cream_victim:
        score = 500
        details.append({
            "score": score,
            "title": f"cream hacking victim",
            "reason": ""
        })
        total_score += score
    if item.badger_victim:
        score = 500
        details.append({
            "score": score,
            "title": f"badger hacking victim",
            "reason": ""
        })
        total_score += score
    if item.anubis_victim:
        score = 500
        details.append({
            "score": score,
            "title": f"anubis rug victim",
            "reason": ""
        })
        total_score += score

    # ETH deposit
    if item.eth_deposit:
        score = 500
        details.append({
            "score": score,
            "title": f"eth2.0 depositor",
            "reason": ""
        })
        total_score += score

    # airdrop
    if item.loot_claim:
        score = 500
        details.append({
            "score": score,
            "title": f"loot minter",
            "reason": ""
        })
        total_score += score

    score = 0
    if 0 < item.ens_claim < 100:
        score = 200
        details.append({
            "score": score,
            "title": f"ens supporter",
            "reason": ""
        })
    elif 100 <= item.ens_claim < 200:
        score = 300
        details.append({
            "score": score,
            "title": f"ens supporter",
            "reason": ""
        })
    elif item.ens_claim >= 200:
        score = 500
        details.append({
            "score": score,
            "title": f"ens supporter",
            "reason": ""
        })
    total_score += score

    # max gas
    if item.gas_max > 1:
        score = 200
        details.append({
            "score": score,
            "title": f"max gas tx is{item.gas_max}",
            "reason": ""
        })
        total_score += score

    # developer
    score = item.contract_deploy * 100
    if score > 1000:
        score = 1000
    if score > 0:
        details.append({
            "score": score,
            "title": f"deploy {item.contract_deploy} contract",
            "reason": ""
        })
    total_score += score

    double_check = 0
    for row in details:
        double_check += row["score"]
    if total_score != double_check:
        print("ERROR!!", total_score, double_check)
        raise Exception("ERROR")

    return int(total_score), details



class APEScore(Model):
    address = CharField()
    creature = CharField()
    score = DoubleField()

    class Meta:
        database = db
        db_table = 'metacraft_ape_score'


def save(step):
    start = 0
    while start <= 100312711:
        start = redis_conn.incr("current_index", step)
        print("start", start)
        query = Summary.select().where(Summary.id.between(start, start + step - 1))
        rows = []
        for item in query:
            if item.gas_all > 20 and item.project_num > 20 and item.opensea_buy > 50:
                creature = "EnderDragon"
            elif item.polynet_victim > 0 or item.cream_victim > 0 or item.badger_victim > 0 or item.anubis_victim > 0:
                creature = "Wither"
            elif item.loot_claim > 0 or item.ens_claim > 200:
                creature = "Dolphin"
            elif item.contract_deploy > 2:
                creature = "Enderman"
            elif item.eth_deposit > 0:
                creature = "IronGolem"
            elif item.tx_fail_num / item.tx_num > 0.2:
                creature = "Creeper"
            elif item.tx_num > 300 and item.project_num > 10:
                creature = "Bee"
            elif item.opensea_buy >= 5:
                creature = "Axolotl"
            elif item.tx_num < 50 and 0 < item.project_num <= 5:
                creature = "Turtle"
            elif item.project_num >= 6:
                creature = "Cow"
            else:
                creature = "Slime"

            score, detail = cal_score(item)

            rows.append({
                "address": item.address,
                "creature": creature,
                "score": score
            })

        if rows:
            with db.atomic() as transaction:
                APEScore.insert_many(rows).execute()
                print(f"success to save {len(rows)}")


if __name__ == '__main__':

    thread_list = []
    step = 1000
    redis_conn.set("current_index", 50156355 - step)
    for i in range(4):
        t = multiprocessing.Process(target=save, args=(step,))
        thread_list.append(t)
    for t in thread_list:
        t.start()
    for t in thread_list:
        t.join()

    print("done.")
