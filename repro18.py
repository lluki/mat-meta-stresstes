#!/usr/bin/python3
import psycopg2,time
PSQL="postgres://materialize@localhost:6875/materialize"

def step1():
    conn = psycopg2.connect(PSQL)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("CREATE SOURCE auction_house FROM LOAD GENERATOR AUCTION FOR ALL TABLES WITH (SIZE = '2');")
        cur.execute("CREATE MATERIALIZED VIEW bids_mv AS select *,amount+1 as new_amount from bids;")

def step2():
    conn = psycopg2.connect(PSQL)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("DROP CLUSTER REPLICA default.r1;")

def step3():
    conn = psycopg2.connect(PSQL)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("CREATE CLUSTER REPLICA default.r1 SIZE '1';")



print("Setup!")
step1()
print("Setup done. Waiting 300s")
time.sleep(300)
print("Disruption!")
step2()
print("Disruption done, waiting 300s")
time.sleep(300)
print("Recreate replica!")
step3()
print("Recreate replica done")
time.sleep(300)
print("Waited another 300s")
