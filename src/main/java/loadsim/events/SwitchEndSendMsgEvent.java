package loadsim.events;

import loadsim.QueryTask;
import loadsim.SearchLoadModel;
import desmoj.core.simulator.Event;

public class SwitchEndSendMsgEvent extends Event<QueryTask> {

  @SuppressWarnings("unused")
  private SearchLoadModel owner;
  
  public SwitchEndSendMsgEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(QueryTask who) {
    who.getDestination().getSwitch().sentMessage(who);
  }

}
