import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from stats import read, parse
from math import floor, ceil

target = read()
# files = ["queue_st.csv", "queue_sc.csv", "queue_nd.csv", "queue_mnd.csv"]
files = ["queue_st.csv", "queue_mnd.csv"]


def pd_parse(file):
    df = pd.read_csv(file)
    # df['index'] = df.apply(lambda row: row.name, axis=1)
    df['actualRunningTime'] = df.apply(lambda row: row['queryEnd-7'] - row['queryArrival-1'], axis=1)
    df['expectedRunningTime'] = df.apply(lambda row: target[row['queryNum-0'].astype(int)], axis=1)
    df['runningTimeDiff'] = df.apply(lambda row:  row['actualRunningTime'] - row['expectedRunningTime'], axis=1)
    # df['mainQueueTime'] = df.apply(lambda row: row['searchArrival-2'] - row['queryArrival-1'], axis=1)
    # df['mergeQueueTime'] = df.apply(lambda row: row['mergeBegin-6'] - row['mergeArrival-5'], axis=1)
    # df['mergeTime'] = df.apply(lambda row: row['queryEnd-7'] - row['mergeBegin-6'], axis=1)

    df['searchQueueTime'] = df.apply(lambda row: row['searchEnd-4'] - row['searchArrival-2'] - target[row['queryNum-0'].astype(int)], axis=1)
    return df[['searchQueueTime']]

    # return df[['actualRunningTime', 'expectedRunningTime']]
    # return df[['runningTimeDiff']]


def plot_scatter():
    fig, axes = plt.subplots(nrows=1, ncols=len(files))

    for i, df in enumerate([pd.read_csv(x) for x in files]):
        df['latency'] = df.apply(lambda row: row['queryEnd-7'] - row['queryArrival-1'], axis=1)
        df.plot(ax=axes[i], kind="scatter", x='queryEnd-7', y='latency')
        axes[i].set_title(files[i])
    
    plt.show()
# plot_scatter()

def plot_trailing(): 
    fig, axes = plt.subplots(nrows=1, ncols=len(files))
    query_workload = read(1)
    for index, df in enumerate([pd.read_csv(x) for x in files]):
        # if index > 0: continue
        p = 0
        cnt = 0
        w = 300
        ws = ceil(df['queryEnd-7'].max() / w)
        
        res = pd.DataFrame({ 'time': list(map(lambda x: (x+1) * ws, [x for x in range(0, w)])) })
        res['latency'] = 0.0
        res['workload'] = 0.0

        i = 0
        l_cnt = w_cnt = 0
        for row in df.itertuples():
            if floor(row[8]) < ws*(i+1):
                l_cnt+=1
            else:
                if l_cnt != 0:
                    res.at[i, 'latency']/=l_cnt
                l_cnt = 0
                i+=1
            res.at[i, 'latency']+=row[8]-row[2]
        res.at[i, 'latency']/=l_cnt

        i = 0
        for row in df.sort_values(by=['queryNum-0']).itertuples():
            if floor(row[2]) < ws*(i+1):
                w_cnt+=1
            else:
                if w_cnt != 0:
                    res.at[i, 'workload']/=w_cnt
                w_cnt = 0
                i+=1
            res.at[i, 'workload']+=query_workload[int(row[1])]
        res.at[i, 'workload']/=w_cnt

        res.plot(ax=axes[index],kind="line", x='time', ylim=(0, 500))
        axes[index].set_title(files[index])
    plt.show()

# plot_trailing()



def plot_waittime():
    
    fig, axes = plt.subplots(nrows=1, ncols=len(files))

    if True:
        ys = map(lambda x: pd.DataFrame({'searchQueueTime':parse(x, 4)}), files)
    else:
        ys = map(pd_parse, files)

    for i, df in enumerate(ys):
        df.iloc[3000:3500].plot(ax=axes[i], kind="bar", stacked=True)
        axes[i].get_xaxis().set_visible(False)
        axes[i].set_title(files[i])
    plt.show()

# plot_waittime()


def plot_shard_waittime(file):
    src = pd.read_csv(file)
    src.dropna()
    df = pd.DataFrame()
    df['searchTime'] = src.apply(lambda row: row['shardFinish-2'] - row['shardStart-1'], axis=1)
    df['waitTime'] = src.apply(lambda row: row['shardStart-1'] - row['shardArrival-0'], axis=1)
        
    fig, axes = plt.subplots(nrows=1, ncols=1)
    df.iloc[5000:5500].plot(ax=axes, kind="bar", stacked=True)

    # for i, df in enumerate(ys):
    axes.get_xaxis().set_visible(False)
    plt.show()

plot_shard_waittime('shards_queue_st.csv')