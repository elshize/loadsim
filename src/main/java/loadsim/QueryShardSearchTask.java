package loadsim;

import loadsim.Query.QueryState;
import loadsim.events.ShardSearchArrivalEvent;
import loadsim.events.ShardSearchEndEvent;
import java.util.concurrent.TimeUnit;

import desmoj.core.simulator.Event;


/**
 * Represents a shard selection for a query with the cost for that shard.
 * @author yubink
 *
 */
public class QueryShardSearchTask extends QueryTask {

  public QueryShardSearchTask(SearchLoadModel owner, String name, boolean showInTrace, 
      Query query, String shard, long cost) {
    super(owner, name, showInTrace, query);
    this.query = query;
    this.shard = shard;
    //this.destMachine = destMachine; // machine where the shard to be searched is located
    this.cost = cost; // cost in posting lists
    this.time = SearchLoadModel.convertCostToTime(cost, query);
  }
  
  public void process(JobThread thread) {
    query.updateState(QueryState.SHARD_SEARCH_STARTED);
    this.shardStartTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
    ShardSearchEndEvent end = new ShardSearchEndEvent(owner, "ShardSearchDone " + getName(), traceIsOn());
    end.schedule(this, thread, SearchLoadModel.convertCostToTimeSpan(cost, query));
  }

  public Query query;
  public String shard;
  public Machine destMachine;
  public long cost;
  public double time;
//   public double optimal;
  public double expected;
  public boolean done = false;
  public double shardArrivalTime;
  public double shardStartTime;
  public double shardFinishTime;

  @Override
  public Event<QueryShardSearchTask> generateArrivalEvent() {
    return new ShardSearchArrivalEvent(owner, "ShardSearchArrival " +getName(), traceIsOn());
  }

  @Override
  public Machine getDestination() {
    return destMachine;
  }
  
  public void setDestination(Machine machine) {
    this.destMachine = machine;
  }

  @Override
  public Machine getSource() {
    // machine where resource selection was done
    return query.getMergeMachine();
  }

  public double getShardWaitTime() {
    return shardFinishTime - shardArrivalTime; 
  }
  
  public void setArrivalTime() {
    shardArrivalTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
  }

  public void setFinishTime() {
    shardFinishTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
  }
}