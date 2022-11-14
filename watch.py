#!/usr/bin/python3
import psutil,re,time,argparse,pandas as pd,functools
import matplotlib.pyplot as plt

LOG_FILE = "/tmp/matmem.csv"



def capture():
# Truncate log file and write header
    print(f"Writing logfile to {LOG_FILE}")
    start = time.time()
    with open(LOG_FILE,"w") as outf:
        outf.write("time,pid,name,cpu,mem\n")

    while True:
        # check for new processes to watch

        log_lines = []
        now = int(time.time() - start)
        for p in psutil.process_iter():
            name = p.name()
            if re.match("environmentd|computed|storaged", name):
                with p.oneshot():
                    # Find a nicer name for computed
                    if re.match("computed|storaged", name):
                        for x in p.cmdline():
                            if x.startswith("--pid-file"):
                                name = re.split(r"[\./]",x)[-2]

                    # Log data
                    log_lines.append("{},{},{},{},{}".format(
                        now,
                        p.pid,
                        name,
                        p.cpu_percent(),
                        p.memory_info().vms))

        with open(LOG_FILE,"a") as outf:
            outf.write("\n".join(log_lines))
            outf.write("\n")

        time.sleep(1)

def plot():
    all_df = pd.read_csv(LOG_FILE)
    procs = set(all_df["name"])
    mem_dfs = []
    for proc in procs:
        mem_dfs.append(all_df[all_df["name"] == proc][ ["time","mem"] ].rename(columns={"mem":proc}))
    mem_df = functools.reduce(lambda left,right: pd.merge(left,right,on="time",how="outer"), mem_dfs)

    # Divide all columns except first by 1024*1024 to turn them into MiB
    mem_df.iloc[:, 1:] = mem_df.iloc[:,1:].div(1024.0*1024.0, axis=0)
    ax = mem_df.plot(x="time",y=procs)
    ax.set_xlabel("Time [s]")
    ax.set_ylabel("Mem [MiB]")
    plt.show()


parser = argparse.ArgumentParser()
parser.add_argument('-p', '--plot', action='store_true', default=False, help="Plot logfile")
parser.add_argument('-c', '--capture', action='store_true', default=False, help="Capture logfile")
args = parser.parse_args()

if args.plot:
    plot()
elif args.capture:
    capture()
else:
    parser.print_help()
