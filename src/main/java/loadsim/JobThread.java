package loadsim;

import desmoj.core.simulator.Entity;
import desmoj.core.simulator.Model;

public class JobThread extends Entity {

  /** Machine this thread belongs to **/
  public Machine machine; 
  
  public JobThread(Model owner, String name, boolean showInTrace, Machine machine) {
    super(owner, name, showInTrace);
    this.machine = machine;
  }

}
