from random import randint, shuffle

shardNum=8000
machineNum=100
shardCopy=4


def randomness_test(shardNum, machineNum, shardCopy):
    # tests the randomness of the generated map
    shardList = [x for x in range(0, shardNum)] * shardCopy
    machineMap = [[] for x in range(0, machineNum)]
    shardMap = [[] for x in range(0, shardNum)]
    shardPerMachine = len(shardList)/machineNum
    shuffle(shardList)
    for i in range(0, len(shardList)):
        x = i
        while len(machineMap[x%machineNum]) == shardPerMachine or shardList[i] in machineMap[x%machineNum]:
            if x > 2*machineNum:
                break
            x+=1
        machineMap[x%machineNum].append(shardList[i])
        shardMap[shardList[i]].append(x%machineNum)

    cnt = 0
    for i, machine in enumerate(machineMap):
        back_ups = set()
        for shard in machine:
            back_ups.update(shardMap[shard])
        back_ups.remove(i)
        if (len(back_ups) < shardPerMachine):
            cnt+=1
    print("Shard %d | Machine %d | Randomness %.2f" % (shardNum, machineNum, 1-cnt/machineNum))

for x in range(1000, 4000, 50):
    # tests randomness with shard number range
    randomness_test(x, machineNum, shardCopy)
