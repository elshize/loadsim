package loadsim;

import desmoj.core.simulator.Event;
import loadsim.Query.QueryState;
import loadsim.events.CentralQueryTaskArrivalEvent;
import loadsim.events.QuerySelectionEndEvent;

public class QueryResourceSelectionTask extends QueryTask {

  public QueryResourceSelectionTask(SearchLoadModel owner, String name, boolean showInTrace,
      Query query) {
    super(owner, name, showInTrace, query);
  }

  @Override
  public void process(JobThread thread) {
    query.updateState(QueryState.STARTED);
    query.setMergeMachine(thread.machine);
    
    // schedule end of resource selection processing
    QuerySelectionEndEvent end = new QuerySelectionEndEvent(owner, "SelectionDone " + getName(), traceIsOn());
    end.schedule(this, thread,
        SearchLoadModel.convertCostToTimeSpan(query.getSelectionCost(), query));
  }

  @Override
  public Event<QueryResourceSelectionTask> generateArrivalEvent() {
    return new CentralQueryTaskArrivalEvent(owner, "bad new Q", true);
  }

  @Override
  public Machine getDestination() {
    return null;
  }

  @Override
  public Machine getSource() {
    return null; // resource selection tasks are generated outside of the simulation
  }

}
