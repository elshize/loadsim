package loadsim.events;

import loadsim.SearchLoadModel;
import desmoj.core.simulator.ExternalEvent;
import desmoj.core.simulator.TimeSpan;


public class LoadMonitorEvent extends ExternalEvent {

  private SearchLoadModel model;  
  private int interval;
  private double maxSlope;
  
  public LoadMonitorEvent(SearchLoadModel model, String name, boolean showInTrace, int interval, double maxSlope) {
    super(model, name, showInTrace);
    this.model = model;
    this.interval = interval;
    this.maxSlope = maxSlope;
  }

  @Override
  public void eventRoutine() {
//    model.centralQueueSizes.update(model.centralQueue.size());
    
    if (model.queryGen.finished) {
      // Once all the queries have been queued, take a look at the central queue trend
      // if it's backed up, quit.    
      
      double ystdev = model.centralQueueTally.getStdDev();
      
      int xsize = (int)model.centralQueueTally.getObservations();
      double xmean = ((double)xsize)/2;
      
      double xstdev = 0;
      for (int i = 0; i < xsize; i++) {
        xstdev += Math.pow((i-xmean), 2);
      }
      xstdev = Math.sqrt(xstdev/xsize);
      
      //slope is b = r sy/sx, where r = correlation
      double corr = model.centralQueueTally.getCorrelation();
      double slope = corr * ystdev/xstdev;

      if (slope > maxSlope) {
        System.err.println("Main search queue overloaded: "+slope);
        model.abort = true;
        return;
      }

    } else {
      LoadMonitorEvent event = new LoadMonitorEvent(model, "load monitor", traceIsOn(), interval, maxSlope);
      event.schedule(new TimeSpan(interval));
    }
    
//    if (model.queryQueueOverLoaded()) {
//      System.err.println("Main search queue overloaded.");
//      model.abort = true;
//      return;
//    }
//
//    for (Machine m : model.machineMap.values()) {
//      if (m.searchOverloaded()) {
//        System.err.println("Machine " + m.id + " search overloaded.");
//        model.abort = true;
//        return;
//      }
//      
//      if (m.mergeOverloaded()) {
//        System.err.println("Machine " + m.id + " merge overloaded.");
//        model.abort = true;
//        return;
//      }
//    }
//    

  }

}
