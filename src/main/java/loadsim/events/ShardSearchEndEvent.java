package loadsim.events;

import loadsim.JobThread;
import loadsim.Query.QueryState;
import loadsim.QueryMergeTask;
import loadsim.QueryShardSearchTask;
import loadsim.SearchLoadModel;
import desmoj.core.simulator.EventOf2Entities;

public class ShardSearchEndEvent extends EventOf2Entities<QueryShardSearchTask, JobThread> {

  private SearchLoadModel owner;

  public ShardSearchEndEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(QueryShardSearchTask shard, JobThread thread) {
    // search shard
    shard.getQuery().updateState(QueryState.SHARD_SEARCH_DONE);
    shard.setFinishTime();
    thread.machine.releaseThread(shard, thread);
    
    QueryMergeTask task = new QueryMergeTask(owner, shard.getName() + " merge", true, 
        shard, thread.machine);
    thread.machine.sendMessage(task);
  }

}
