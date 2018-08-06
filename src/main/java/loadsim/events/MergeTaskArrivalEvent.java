package loadsim.events;

import loadsim.Query.QueryState;
import loadsim.QueryMergeTask;
import loadsim.SearchLoadModel;
import desmoj.core.simulator.Event;

public class MergeTaskArrivalEvent extends Event<QueryMergeTask> {
  
  @SuppressWarnings("unused")
  private SearchLoadModel owner;
  
  public MergeTaskArrivalEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(QueryMergeTask who) {
    who.getQuery().updateState(QueryState.MERGE_ARRIVAL);
    who.getDestination().queueTask(who);
  }

}
