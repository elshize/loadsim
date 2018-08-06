package loadsim.events;

import loadsim.Query.QueryState;
import loadsim.QueryShardSearchTask;
import loadsim.SearchLoadModel;
import desmoj.core.simulator.Event;

public class ShardSearchArrivalEvent extends Event<QueryShardSearchTask> {
  
  @SuppressWarnings("unused")
  private SearchLoadModel owner;

  public ShardSearchArrivalEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(QueryShardSearchTask shard) {
    shard.getQuery().updateState(QueryState.SHARD_SEARCH_ARRIVAL);
    shard.setArrivalTime();
    owner.writer.update(shard);
    shard.getDestination().queueTask(shard);    
  }

}
