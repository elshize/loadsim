package loadsim;

import loadsim.Query.QueryState;
import loadsim.events.MergeTaskArrivalEvent;
import loadsim.events.QueryDoneEvent;
import desmoj.core.simulator.Event;

/**
 * Represents a finished shard search being returned to the originating machine for merge. An 
 * actual merge task may not be scheduled at the merge machine when it is received. A merge task is
 * actually scheduled at the merge machine when the merge task being received is the results for the
 * last outstanding shard that still needed to be searched.
 * 
 * @author yubink
 *
 */
public class QueryMergeTask extends QueryTask {
  QueryShardSearchTask task;
  Machine sourceMachine; // machine that generated this task

  public QueryMergeTask(SearchLoadModel owner, String name, boolean showInTrace, 
      QueryShardSearchTask task, Machine source) {
    super(owner, name, showInTrace, task.getQuery());
    this.task = task;
    this.sourceMachine = source;
  }

  @Override
  public void process(JobThread thread) {
    // merge results were sent back & processed; search of shard is now done
    task.done = true;
    
    // if this is the last arriving shard search merge request, begin merge
    if (query.allShardsSearched()) {
      query.updateState(QueryState.MERGE_STARTED);
      QueryDoneEvent done = new QueryDoneEvent(owner, "QueryDone " + task.getName(), traceIsOn());
      done.schedule(this, thread, SearchLoadModel.getMergeCost(query));
    } else {
      thread.machine.releaseThread(this, thread);
    }
  }

  @Override
  public Event<QueryMergeTask> generateArrivalEvent() {
    return new MergeTaskArrivalEvent(owner, "MergeArrival " + task.getName(), true);
  }

  public QueryShardSearchTask getShardTask() {
    return task;
  }

  @Override
  public Machine getDestination() {
    return query.getMergeMachine();
  }

  @Override
  public Machine getSource() {
    return sourceMachine;
  }
}
