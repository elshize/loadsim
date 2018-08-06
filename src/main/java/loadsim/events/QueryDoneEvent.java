package loadsim.events;

import loadsim.JobThread;
import loadsim.Query.QueryState;
import loadsim.QueryMergeTask;
import loadsim.SearchLoadModel;
import desmoj.core.simulator.EventOf2Entities;

public class QueryDoneEvent extends EventOf2Entities<QueryMergeTask, JobThread> {
  
  @SuppressWarnings("unused")
  private SearchLoadModel owner;

  public QueryDoneEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(QueryMergeTask task, JobThread thread) {
    task.getQuery().updateState(QueryState.DONE);
    thread.machine.releaseThread(task, thread);
  }

}
