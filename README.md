# Loadsim

## Introduction

This repository contains a discrete event simulator (developed based the one used in the paper [Efficient Distributed Selective Search](http://boston.lti.cs.cmu.edu/appendices/jir17-yubink/loadsim/)) for simulating the performance of the different queues and query routing strategies.

It also contains some scripts to collect and graph the results of the simulations (e.g. latency distribution, average workload over time, trailing average latency, shard search cost/wait cost ratio and more) and a bash script to automate the simulations with different parameters.

## How to use

### Running for the first time

Make sure `mq.10k.clean.or.rankcost`(shard#/postingListLength pair for each shard selected with an empty line seperating shards for different queries) is in the your working directory.

Generate `shardcost.out` (query sequence) and `shardmap.out` (shard-to-machine map) for the simulator:

    python queryGen.py

Tune the variables in `sample.param`

Build the simulator and start simulations:
    
    ./commands.sh -c

After the simulations finish, data would be exported to `queue_*.csv` and `shards_queue_*.csv` in your working directory.

### Stats and Graphs

To get the latency distribution run:

    python3 stats.py

To get the trailing average, scatter graph and more, run `plotter.py` after uncommenting the corresponding function calls.

### Params

There are currently three dispatching five queuing strategies implemented.

Implementations of queues can be found in `Machine.java` and those of routing strategies' can be found in `QuerySelectionEndEvent.java`

    DISPATCH_STRATEGY = 0:  Select a machine randomly from machines that have idle threads and dispatch
    
    DISPATCH_STRATEGY = 1:  Select a machine that has the lowest expected wait time

    DISPATCH_STRATEGY = 2:  Calculate query optimal by computing the maximum of the sum of 
                            shard.searchTime and shortest machine.waitTime
                            Schdule ShardSearchTask to a machine that maximizes machine.waitTime while
                            satifying the constraint that shard.searchTime+machine.waitTime <= queryOptimal

    queueOrder = 0:         FIFO
    
    queueOrder = 1:         Least Cost First

    queueOrder = 2:         Earliest Deadline First
                            deadline = query.searchArrivalTime + shard.searchTime

    queueOrder = 3:         deadline = query.searchArrivalTime + shard.queryOptimal

    queueOrder = 4:         FIFO with task swapping
                            Shard        S:     incoming shard search task
                            Queue<Shard> Q[N]:  machine search queue of length N
                            Start from the end the the queue, i = N
                            Let Q[i].expected = Q[i].searchTime + machine.waitTime
                            While Q[i].expected + S.searchTime <= Q[i].query.optimal:
                                swap; --i; Q[i].expected+=S.searchTime;
                            else:
                                insert at Q[i];
      


### More Info

You can also run the simulator with a random seed:

    ./commands.sh -s [seed]

If you want to run different simulations in parallel, change this line in `commands.sh`:

    java -jar target/loadsim-0.1.0.jar "sample.param_$q" 'sweep' > "temp_$q.log"  && rm "sample.param_$q" "temp_$q.log"

To:

    nohup java -jar target/loadsim-0.1.0.jar "sample.param_$q" 'sweep' &> "temp_$q.log"  && rm "sample.param_$q" "temp_$q.log" &

To orchestrate the simulations with different parameters, change `paramGen.py` 