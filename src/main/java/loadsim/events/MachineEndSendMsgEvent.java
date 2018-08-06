package loadsim.events;

import loadsim.QueryTask;
import loadsim.SearchLoadModel;
import desmoj.core.simulator.Event;

public class MachineEndSendMsgEvent extends Event<QueryTask> {
  
  @SuppressWarnings("unused")
  private SearchLoadModel owner;

  public MachineEndSendMsgEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(QueryTask task) {
    task.getSource().sentMessage(task);
  }

}
