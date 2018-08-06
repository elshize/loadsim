package loadsim.events;

import loadsim.QueryTask;
import loadsim.SearchLoadModel;
import loadsim.Switch;
import desmoj.core.simulator.EventOf2Entities;

public class SwitchMsgReceivedEvent extends EventOf2Entities<Switch, QueryTask> {
  
  @SuppressWarnings("unused")
  private SearchLoadModel owner;
  
  public SwitchMsgReceivedEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
  }

  @Override
  public void eventRoutine(Switch networkSwitch, QueryTask task) {
    networkSwitch.sendMessage(task);
  }

}
