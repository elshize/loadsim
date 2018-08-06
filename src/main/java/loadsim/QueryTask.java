package loadsim;

import desmoj.core.simulator.Entity;
import desmoj.core.simulator.Event;

public abstract class QueryTask extends Entity {

  protected Query query;
  protected SearchLoadModel owner;
  
  public QueryTask(SearchLoadModel owner, String name, boolean showInTrace, Query query) {
    super(owner, name, showInTrace);
    this.query = query;
    this.owner = owner;
  }
  
  public Query getQuery() {
    return query;
  }

  public abstract void process(JobThread thread);
  
  public abstract Event<? extends QueryTask> generateArrivalEvent();
  
  @SuppressWarnings("unchecked")
  public static <T extends QueryTask> Event<T> generateArrivalEvent(T task) {
    return (Event<T>) task.generateArrivalEvent();
  }
  
  public abstract Machine getDestination();
  
  public abstract Machine getSource();
}
