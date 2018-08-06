from random import randint

with open("mq.10k.clean.or.rankcost", "r") as infile:
    cnt = 0
    shards = 0
    terms = 0
    total = 0
    for i, line in enumerate(infile):
        if line == '\n':
            cnt = 0
            terms+=1
            total+=1
        else:
            if cnt > 4: continue
            shard, cost = line.split(' ')
            shards+=float(cost)
            cnt+=1
        
    print((shards/1067.701 + terms*4)/total)