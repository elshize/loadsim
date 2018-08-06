import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import wasserstein_distance
import argparse, pandas as pd

queueType=4
modeNum=5
queryArrivalRate=400
coreNum=64

"""
    Log stats (mean, 50th percentile, 75th, 95th, utilization) 
    for different queues and the distance of their distributions to the optimal
"""

def parse(filename, mode):
    """
        Reads csv files and extracts info into an array 
    """
    y = []
    if mode == modeNum-1:
        with open("shards_"+filename, "r") as infile:
            # print('calling')
            infile.readline()
            for line in infile:
                if len(line.strip())==0: continue
                q = [float(x) for x in line.strip().split(',')]        
                if q[3] == 1.0:
                    y.append(q[1]-q[0])
            return y

    with open(filename, "r") as infile:
        infile.readline()
        for line in infile:
            q = [float(x) for x in line.strip().split(',')]
            if mode == 0:
                # end-to-end time
                r = q[7]-q[1]
            elif mode == 1:
                r = q[3]-q[2]
            elif mode == 2:
                # total time query spent in shard search (incl. waiting in queue)
                r = q[4]-q[2]
            elif mode == 3:
                r = q[6]-q[5]
            y.append(r)
    return y

def read(mode=0):
    """
        mode=0:
            Returns an array of minimum end-to-end cost per query
        mode=1:
            Returns an array of workload per query
    """
    y = []
    t = 1067.701
    total = 0
    with open("shardcost.out", "r") as infile:
        cnt = 0
        s_cost = 0
        cost = 0
        qn = 0
        for line in infile:
            cnt+=1
            if cnt == 1:
                linearr = line.split(' ')
                query_num, selection_cost, *query_term  = line.split(' ')
                qn = int(query_num)
                s_cost=len(query_term)*0.4+int(selection_cost)/t
            elif line == '\n':
                total+=1
                if qn >= 0: y.append(cost+s_cost)
                cnt = cost = s_cost = 0
            else:
                try:
                    shard, search_cost = line.split(' ')
                except:
                    print(line)
                if mode == 0:
                    cost=max(cost, int(search_cost)/t)
                else:
                    cost+=int(search_cost)/t
    return y

def plot():
    fig, all_axs = plt.subplots(modeNum, queueType+1, sharey=True, tight_layout=True)
    n_bins = 20

    files = ["queue_st.csv", "queue_sc.csv", "queue_nd.csv", "queue_mnd.csv"]
    titles = [
        "End-to-End time",
        "Query search queue time",
        "Query search total time",
        "Merge queue time",
        "Shard queue time"
    ]
    for ax, row in zip(all_axs[:,0], titles):
        ax.set_xlabel(row, rotation=0, size='medium')
    
    target = [read()]
    for m in range(0, modeNum):
        ys = target + list(map(lambda x: parse(x, m), files))
        ws = []
        for i, y in enumerate(ys):
            y = np.array(y)
            if m == 0 and i != 0: ws.append("{:.4f}".format(wasserstein_distance(y, ys[0])))
            axs = all_axs[m][i]
            perc = np.percentile(y, [0.0, 25.0, 50.0, 75.0, 95.0, 99.0, 100.0])
            
            # axs.axvline(y.mean(), color='#05668D', linestyle='dashed', linewidth=2)
            # axs.axvline(perc[2], color='#00A896', linestyle='dashed', linewidth=2)
            # axs.axvline(perc[3], color='#02C39A', linestyle='dashed', linewidth=2)
            # axs.axvline(perc[4], color='#F0F3BD', linestyle='dashed', linewidth=2)
            # axs.axvline(perc[5], color='#f2f2f2', linestyle='dashed', linewidth=2)

            data = [y.mean(), perc[2], perc[3], perc[4], perc[5]]
            d = map(lambda x: "{:4.2f}".format(x), data)
            if m == 0:
                if i > 0:
                    df = pd.read_csv(files[i-1])
                    row = df.shape[0]
                    l = round(row/2)
                    r = round(row/4*3)
                    # µ = (df.iloc[r, 7] - df.iloc[l, 7])*queryArrivalRate/(r-l)/1000
                    µ = queryArrivalRate/1000 * data[0] / coreNum
                    d = (*d, "{:.4f}".format(µ))
                print((['optimal.csv']+files)[i], '', *d)
            axs.legend(["{:.2f}".format(x) for x in data])

        if m==0: print("wasserstein_distance: ", " ".join(ws))
    return
    # plt.show()

if __name__ == "__main__":
    plot()
# read()

