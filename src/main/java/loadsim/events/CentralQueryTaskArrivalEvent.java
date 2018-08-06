package loadsim.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import loadsim.Machine;
import loadsim.QueryResourceSelectionTask;
import loadsim.SearchLoadModel;
import loadsim.Query.QueryState;
import desmoj.core.simulator.Event;

public class CentralQueryTaskArrivalEvent extends Event<QueryResourceSelectionTask> {
  
  private SearchLoadModel owner;
  private Random rnd;
  
  public CentralQueryTaskArrivalEvent(SearchLoadModel owner, String name, boolean showInTrace) {
    super(owner, name, showInTrace);
    this.owner = owner;
    this.rnd = new Random(owner.RANDOM_SEED*11);
  }

  @Override
  public void eventRoutine(QueryResourceSelectionTask who) {
    who.getQuery().updateState(QueryState.QUEUED);    
    owner.centralQueue.insert(who);
    
    if (who.getQuery().trackQuery()) {
      owner.centralQueueTally.update(owner.centralQueue.size());
    }
    
    sendTraceNote("Query queue length: " + owner.centralQueue.size());
    
    List<Machine> allMachines = new ArrayList<>(owner.selectMachines);
    Collections.shuffle(allMachines, rnd);
    
    for (Machine m : allMachines) {
      if (m.hasIdleThread()) {
        QueryResourceSelectionTask currTask = owner.centralQueue.removeFirst();
        m.startNewQuery(currTask);
        break;
      }
    }
    
  }
}
