import os
import itertools
from random import randint, shuffle, seed
from sets import Set

randomSeed=1000                 # ensures result-param consistency 
queryNum=-1000                  # warm-up query count
machineNum=8
shardCopyNum=3
shardSelected=5                 # shard selected for each query
inputRepetition=2               # number of times the input is repeated
infile="mq.10k.clean.or.rankcost"

cnt=0
seed(randomSeed)
shard_map = Set()

"""
    Generates shardcost.out:   query sequence       
          and shardmap.out:    shard-to-machine map
"""

def getNextQuery():
    # randomly generates query terms (1~4) and selection cost (2000~4000)
    global queryNum
    global cnt
    cnt = 0 
    queryNum+=1
    qterm=tuple(["qterm%d" % x for x in range(1, randint(2,5))])
    return ' '.join(str(i) for i in ((queryNum, randint(2000, 4000)) + qterm))+'\n'

# selects top shards and generates query sequence
with open("shardcost.out", "w") as outfile:
    for i in range(0,inputRepetition):
        with open(infile, "r") as infile:
            outfile.write(getNextQuery())
            shards = 0
            for line in infile:
                if len(line.strip()) == 0:
                    outfile.write('\n')
                    outfile.write(getNextQuery())
                    # if queryNum > 200: break
                else:
                    if cnt > shardSelected-1: continue
                    # print(line.split(' '))
                    shard, cost = line.split(' ')
                    shard_map.add(shard)
                    outfile.write(' '.join([shard, cost]))
                    cnt+=1
            outfile.write('\n')

# random shard map generation
with open("shardmap.out", "w") as outfile:
    shardNum=len(shard_map)
    shardList = [x for x in range(0, shardNum)] * shardCopyNum
    machineMap = [[] for x in range(0, machineNum)]
    shuffle(shardList)
    for i in range(0, len(shardList)):
        x = i
        while len(machineMap[x%machineNum]) == len(shardList)/machineNum or shardList[i] in machineMap[x%machineNum]:
            if x > 2*machineNum: break
            x+=1
        outfile.write('%s %d\n' % (shardList[i], x%machineNum))