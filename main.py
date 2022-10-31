#!/usr/bin/python3
import psycopg2, threading, string, random, socket, datetime

PSQL="postgres://materialize@localhost:6875/materialize"
SIZE = "2" if "Papa" in socket.gethostname() else "xsmall"
VERBOSE=False

log = open("command_log.txt","w")
log_lock = threading.Lock()
def sqls(conn, cmds, fetch=False):
    log_lock.acquire()
    now = datetime.datetime.now().isoformat()
    for cmd in cmds:
        log.write(now)
        log.write(": ")
        log.write(cmd)
        log.write("\n")
        if VERBOSE:
            print(cmd)
    log_lock.release()
    with conn.cursor() as cur:
        for cmd in cmds:
            cur.execute(cmd)
            if fetch:
                if VERBOSE:
                    print(cur.fetchall())

def sql(conn, cmd, fetch=False):
    sqls(conn, [cmd], fetch=fetch)


class InputInsert():
    weight = 1.0
    next_idx = 10
    def __init__(self):
        self.idx = InputInsert.next_idx
        InputInsert.next_idx += 1
        self.data =  ''.join(random.choice(string.ascii_lowercase) for i in range(10))
    
    def do(self, conn):
        sql(conn, "INSERT INTO input VALUES ({}, '{}')".format(self.idx,self.data))

    def undo(self, conn):
        sql(conn, "DELETE FROM input WHERE id = {}".format(self.idx))

class TableCreate():
    weight = 1.0
    next_idx = 10
    def __init__(self):
        self.idx = TableCreate.next_idx
        TableCreate.next_idx += 1
    
    def do(self, conn):
        sql(conn, "CREATE TABLE t{} (id int, data string)".format(self.idx))

    def undo(self, conn):
        sql(conn, "DROP TABLE t{}".format(self.idx))

class ViewCreate():
    weight = 1.0
    next_idx = 10
    def __init__(self):
        self.idx = ViewCreate.next_idx
        ViewCreate.next_idx += 1
    
    def do(self, conn):
        sqls(conn, [
            "CREATE VIEW tmp_view_{} AS SELECT MAX(id) FROM input".format(self.idx),
            "CREATE DEFAULT INDEX ON tmp_view_{}".format(self.idx)
            ])

    def undo(self, conn):
        sql(conn, "DROP VIEW tmp_view_{} CASCADE".format(self.idx))

class MaterializedViewCreate():
    weight = 0.1
    next_idx = 10
    def __init__(self):
        self.idx = MaterializedViewCreate.next_idx
        MaterializedViewCreate.next_idx += 1
    
    def do(self, conn):
        sqls(conn, [
            "CREATE MATERIALIZED VIEW tmp_mat_view_{} AS SELECT MIN(id) FROM input".format(self.idx),
            ])

    def undo(self, conn):
        sql(conn, "DROP MATERIALIZED VIEW tmp_mat_view_{} CASCADE".format(self.idx))

class Select():
    weight = 2.0
    def do(self, conn):
        sql(conn, "SELECT AVG(id) FROM input", fetch=True)

    def undo(self, _):
        pass

class Subscribe():
    weight = 0.5
    def do_bg(self, conn):
        with psycopg2.connect(PSQL) as conn:
            with conn.cursor() as cur:
                cur.execute("DECLARE c CURSOR FOR SUBSCRIBE mv1")
                while not self.stop.is_set():
                    cur.execute("FETCH ALL c")
                    for row in cur:
                        pass

    def do(self, conn):
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.do_bg, args=(conn,))
        self.thread.start()

    def undo(self, _):
        self.stop.set()
        self.thread.join()

class ReplicaCreate():
    weight = 0.01
    next_idx = 10
    def __init__(self):
        self.idx = ReplicaCreate.next_idx
        ReplicaCreate.next_idx += 1

    def do(self, conn):
        sql(conn, "CREATE CLUSTER REPLICA default.tmp_{} SIZE '{}'".format(self.idx, SIZE))

    def undo(self, _):
        sql(conn, "DROP CLUSTER REPLICA default.tmp_{}".format(self.idx))


# Counts how many Do and Undo's we have performed
class Do():
    done = 0
    def __init__(self, act):
        self.act = act
    def __call__(self, conn):
        self.act.do(conn)
        Do.done += 1

class Undo():
    done = 0
    def __init__(self, act):
        self.act = act
    def __call__(self, conn):
        self.act.undo(conn)
        Undo.done += 1


def setup(conn):
    sqls(conn, [
        "CREATE TABLE IF NOT EXISTS input (id int, data string)",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 AS SELECT count(*) FROM input",
        "CREATE VIEW IF NOT EXISTS v1 AS SELECT count(*) FROM input",
        "CREATE DEFAULT INDEX IF NOT EXISTS ON v1",
    ])
    constructors = [InputInsert, TableCreate, ViewCreate, MaterializedViewCreate, Select, ReplicaCreate]
    actions = random.choices(
            population=constructors, 
            weights=[x.weight for x in constructors],
            k=1000
            )
    pending_dos = [x() for x in actions]
    pending_undos = []
    do_undo_acts = []
    while True:
        has_pending_dos = len(pending_dos) > 0
        has_pending_undos = len(pending_undos) > 0
        if has_pending_dos and has_pending_undos:
            if random.randint(0,1) == 0:
                act = pending_dos.pop()
                do_undo_acts.append(Do(act))
                pending_undos.append(Undo(act))
            else:
                do_undo_acts.append(pending_undos.pop())
        elif has_pending_undos:
            do_undo_acts.append(pending_undos.pop())
        elif has_pending_dos:
            act = pending_dos.pop()
            do_undo_acts.append(Do(act))
            pending_undos.append(Undo(act))
        else:
            break


    start = datetime.datetime.now()
    for i,act in enumerate(do_undo_acts):
        act(conn)
        if i % 10 == 0:
            elapsed = (datetime.datetime.now() - start).seconds
            print("{:>4}s: Performed {:>4} of {:>4} actions so far. Do={:>4}, Undo={:>4}, ObjsAlive={:>4}".format(
                    elapsed, i, len(do_undo_acts), Do.done, Undo.done,
                    Do.done - Undo.done))

conn = psycopg2.connect(PSQL)
conn.autocommit = True
setup(conn)
