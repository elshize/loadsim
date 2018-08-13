package loadsim.events;

import loadsim.JobThread;
import loadsim.Machine;
import loadsim.Query;
import loadsim.Query.QueryState;
import loadsim.QueryResourceSelectionTask;
import loadsim.QueryShardSearchTask;
import loadsim.SearchLoadModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import desmoj.core.simulator.EventOf2Entities;

public class QuerySelectionEndEvent extends EventOf2Entities<QueryResourceSelectionTask, JobThread> {
  
  private SearchLoadModel owner;

  public  QuerySelectionEndEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(QueryResourceSelectionTask task, JobThread thread) {
    thread.machine.releaseThread(task, thread);
    
    Query query = task.getQuery();
    sendTraceNote(query.toString() + " resource selected by " + thread.toString());
    query.updateState(QueryState.RESOURCE_SELECTED);

    // get list of selected shards and queue jobs for searching those shards
    if (owner.DISPATCH_STRATEGY == 2) {
        // query.searchTasks.sort((o1, o2) -> Long.valueOf(o2.cost).compareTo(o1.cost));
        // assert(query.searchTasks.get(0).cost >= query.searchTasks.get(1).cost);
        double max = 0;
        for (QueryShardSearchTask shard : query.searchTasks) {
            double min = 10000000.0;
            for (String machinePrefix: owner.getMachinePrefixList(shard.shard)) {
                String machineId = machinePrefix + "." + thread.machine.mirror;
                Machine m = owner.machineMap.get(machineId);
                if (m.machineQueueSize < min) min = m.machineQueueSize;
            }
            max = Math.max(min+shard.time, max);
        }
        for (QueryShardSearchTask shard : query.searchTasks) {
            Machine dest = null;
            List<Machine> machines = new ArrayList<>();
            for (String machinePrefix: owner.getMachinePrefixList(shard.shard)) {
                String machineId = machinePrefix + "." + thread.machine.mirror;
                Machine m = owner.machineMap.get(machineId);
                machines.add(m);
            }
            machines.sort((o1, o2) -> Double.valueOf(o1.machineQueueSize).compareTo(o2.machineQueueSize));
            double upper = max-shard.time;
            // for (Machine m: machines) {
            //     if (m.machineQueueSize <= upper) dest = m;
            //     else break;
            // }
            if (dest == null) dest = machines.get(0);
            shard.setDestination(dest);
            thread.machine.sendMessage(shard);
        }
    } else {
        for (QueryShardSearchTask shard : query.searchTasks) {
            if (owner.DISPATCH_STRATEGY == 1) {
                double min = 10000000.0;
                Machine dest = null;
                for (String machinePrefix: owner.getMachinePrefixList(shard.shard)) {
                    String machineId = machinePrefix + "." + thread.machine.mirror;
                    Machine m = owner.machineMap.get(machineId);
                    if (m.machineQueueSize < min) {
                        min = m.machineQueueSize;
                        dest = m;
                    }
                }
                shard.expected = shard.time+min;
                query.maxCost = Math.max(shard.expected, query.maxCost);
                shard.setDestination(dest);
                thread.machine.sendMessage(shard);
                // find the actual machine id based on the mirror version of the resource selection machine
            } else {
                String machinePrefix = owner.getMachinePrefix(shard.shard);
                // find the actual machine id based on the mirror version of the resource selection machine
                String machineId = machinePrefix + "." + thread.machine.mirror;
                shard.setDestination(owner.machineMap.get(machineId));
                thread.machine.sendMessage(shard);
            }
        }
        // if (owner.DISPATCH_STRATEGY == 1) {
        //     for(QueryShardSearchTask shard: query.searchTasks) thread.machine.sendMessage(shard);
        // }
    }
  }

}
