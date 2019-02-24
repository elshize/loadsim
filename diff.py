from random import shuffle

def query_last_shard(filename):
    y =[]
    with open("shards_"+filename, "r") as infile:
        shard_index = 0
        line_num = 0
        query_num = 0
        infile.readline()
        for line in infile:
            if len(line.strip()) == 0:
                shard_index = 0
                query_num+=1
                continue
            q = [float(x) for x in line.strip().split(',')]  
            if q[3] == 1.0:
                y.append((shard_index, q[2]-q[1], q[1]-q[0], q[0]))
            shard_index+=1     
        return y

def sc_mnd_diff():
    queue_sc = query_last_shard("queue_sc.csv")
    queue_mnd = query_last_shard("queue_tmnd.csv")
    print("{:4} {:6} {:6} {:6} {:6} {} {}".format('query_num', 'sc_search', 'mnd_search', 'sc_wait', 'mnd_wait', 'sc_index', 'mnd_index'))
    for i, (x, y, z, m) in enumerate(queue_sc):
        tx, ty, tz, tm = queue_mnd[i]
        if x != tx:
            if i < 120:
                if y < ty:
                    print("{:9} {:9.2f} {:10.2f} {:7.2f} {:8.2f} {:8.2f} {:8.2f} {:8} {:9}".format(i, y, ty, z, tz, m, tm, x, tx))
sc_mnd_diff()