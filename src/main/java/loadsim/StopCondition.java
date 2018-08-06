package loadsim;

import java.util.Scanner;

import desmoj.core.simulator.ModelCondition;

public class StopCondition extends ModelCondition {
    SearchLoadModel model; 
    Scanner queryListScanner;
    
    public StopCondition(SearchLoadModel owner, String name, boolean showInTrace,
        Scanner queryListScanner) {
      super(owner, name, showInTrace);
      this.model = owner;
      this.queryListScanner = queryListScanner;
    }

    @Override
    public boolean check() {
        // return false
      // quit early if switch is set
      if (model.abort == true) {
        return true;
      }

      // wait until all queries have been read and finished
      if (queryListScanner.hasNext()) {
        return false;
      }
      
      if (model.queryGen.lastQuery != null && !model.queryGen.lastQuery.isDone()) {
        return false;
      }

      for (Machine mach : model.machineMap.values()) {
        if (mach.idleThreads.size() < mach.getNumCpus()) {
          return false;
        }
      }

      if (!model.networkSwitch.queuesEmpty()) {
        return false;
      }

      if (!model.monitorLoad) {
        // if load monitoring is turned off, don't check for slopes
        return true;
      }
      
      double slope = model.centralQueueTally.getSlope();

//        for (Double i : model.centralQueueTally.lastThousand) {
//          System.err.println(String.format("%.2f", i) + " ");
//        }
//        System.err.println();
//        System.err.println(slope);

      // check if main search queue is overloaded
      if (slope > SearchLoadModel.MAX_SLOPE) {
        System.err.println("Main search queue overloaded: " + slope);
        model.abort = true;
        return true;
      }
      
      // check if any of the machine queues are overloaded
      for (Machine mach : model.machineMap.values()) {
        if (mach.hasSelect) {
//            slope = mach.mergeQueueTally.getSlope(); 
//            if (slope > SearchLoadModel.MAX_SLOPE) {
//              System.err.println(mach.id + " merge queue overloaded: " + slope);
//              model.abort = true;
//              return true;
//            }
        }
        slope = mach.searchQueueTally.getSlope(); 
        if (slope > SearchLoadModel.MAX_SLOPE) {
          System.err.println("Machine: "+ mach.id + " search queue overloaded: " + slope);
          model.abort = true;
          return true;
        }
//          slope = mach.messageQueueTally.getSlope();
//          if (slope > SearchLoadModel.MAX_SLOPE) {
//            System.err.println(mach.id + " msg queue overloaded: " + slope);
//            model.abort = true;
//            return true;
//          }
      }
      return true;
    }
  };