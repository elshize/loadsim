# Loadsim

## Disclaimer

This repository is based on the following code: http://boston.lti.cs.cmu.edu/appendices/jir17-yubink/loadsim
related to the following paper:

_Kim, Yubin, Jamie Callan, J. Shane Culpepper, and Alistair Moffat. "Load-balancing in distributed selective search." In Proceedings of the 39th international ACM SIGIR conference on research and development in information retrieval, pp. 905-908. ACM, 2016._

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

### More Info

You can also run the simulator with a random seed:

    ./commands.sh -s [seed]

If you want to run different simulations in parallel, change this line in `commands.sh`:

    java -jar target/loadsim-0.1.0.jar "sample.param_$q" 'sweep' > "temp_$q.log"  && rm "sample.param_$q" "temp_$q.log"

To:

    nohup java -jar target/loadsim-0.1.0.jar "sample.param_$q" 'sweep' &> "temp_$q.log"  && rm "sample.param_$q" "temp_$q.log" &

To orchestrate the simulations with different parameters, change `paramGen.py` 
