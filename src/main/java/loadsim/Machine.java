package loadsim;

import loadsim.events.MachineEndSendMsgEvent;
import loadsim.events.SwitchMsgReceivedEvent;
import loadsim.utils.CorrelationTally;
import desmoj.core.simulator.Entity;
import desmoj.core.simulator.Queue;
import desmoj.core.simulator.TimeSpan;
import desmoj.core.statistic.Accumulate;
import desmoj.core.statistic.ValueSupplier;

/**
 * Represents a machine in the simulation which has cpus that can handle tasks
 * 
 * @author yubink
 *
 */
public class Machine extends Entity {

  SearchLoadModel owner;

  /**
   * Queue for outgoing messages to different machines.
   */
  private Queue<QueryTask> sendMessageQueue;

  /**
   * Queue for merge tasks which live on each machine where there are central tasks
   */
  private Queue<QueryMergeTask> mergeQueue;

  /**
   * Queue for search tasks which live on each machine where there are search tasks
   */
  private Queue<QueryShardSearchTask> searchQueue;

  /**
   * Number of available parallel cores in this machine.
   */
  Queue<JobThread> idleThreads;

  /**
   * Idle resource selection threads (also does merging)
   */
  public Queue<JobThread> idleSelectThreads;

  /**
   * Idle shard search threads
   */
  public Queue<JobThread> idleSearchThreads;

  // variables that keep track of how many cpus are doing what
  private int mergingCpus = 0;
  private int searchingCpus = 0;
  private int selectingCpus = 0;

  private int numCpus;

  // is the out going message channel occupied?
  private boolean sendBusy = false;

  // variables to track CPU load of this machine
  public Accumulate totalLoad;
  public Accumulate mergeLoad;
  public Accumulate selectLoad;
  public Accumulate searchLoad;
  
  // Tallies to determine of any of the queues overload based on slope
  public CorrelationTally messageQueueTally;
  public CorrelationTally mergeQueueTally;
  public CorrelationTally searchQueueTally;

  // reference to switch that this machine is hooked up to
  public Switch networkSwitch;

  // machine's string id
  public String id;
  public int mirror; 
  boolean hasSelect; // this machine does resource selection
  public double machineQueueSize;
  
  // trackers to see if queues are being overloaded
  private int prevMergeQ = SearchLoadModel.QUEUE_BACKUP_MIN;
  private int mergeGrowth = 0;
  private int prevSearchQ = SearchLoadModel.QUEUE_BACKUP_MIN;
  private int searchGrowth = 0;

  public Machine(SearchLoadModel owner, String id, int mirror, int cpus, Switch networkSwitch,
      boolean hasSelect, boolean showInReport) {
    super(owner, "Machine " + id, showInReport);

    this.id = id;
    this.mirror = mirror;
    this.owner = owner;
    this.networkSwitch = networkSwitch;
    this.numCpus = cpus;
    this.hasSelect = hasSelect;
    this.machineQueueSize = 0.0;

    idleThreads = new Queue<>(owner, "Mach " + id + " threads", showInReport, true);
    for (int i = 0; i < cpus; i++) {
      JobThread newThread = new JobThread(owner, "Thread " + i, true, this);
      idleThreads.insert(newThread);
    }

    // set up utilization tracker for central machine threads
    totalLoad = new Accumulate(owner, id + " load", new ValueSupplier("VS") {
      @Override
      public double value() {
        return 1 - idleThreads.size() / (double) numCpus;
      }
    }, false, true, false);

    if (hasSelect) {
      mergeLoad = new Accumulate(owner, id + " merge load", new ValueSupplier("VS") {
        @Override
        public double value() {
          return mergingCpus / (double) numCpus;
        }
      }, false, true, false);

      selectLoad = new Accumulate(owner, id + " select load", new ValueSupplier("VS") {
        @Override
        public double value() {
          return selectingCpus / (double) numCpus;
        }
      }, false, true, false);

      mergeQueue = new Queue<>(owner, id + " mergeQ", true, true);
      mergeQueueTally = new CorrelationTally(owner, id + " mergeQ tally", true, false);
    }

    searchLoad = new Accumulate(owner, id + " search load", new ValueSupplier("VS") {
      @Override
      public double value() {
        return searchingCpus / (double) numCpus;
      }
    }, false, true, false);
    searchQueue = new Queue<>(owner, id + " searchQ", true, true);
    searchQueueTally = new CorrelationTally(owner, id + " searchQ tally", true, false);

    sendMessageQueue = new Queue<>(owner, id + " sendQ", true, true);
    messageQueueTally = new CorrelationTally(owner, id + " msgQ tally", true, false);
  }

//   public Machine(SearchLoadModel owner, String id, int selectCpus, int searchCpus,
//       boolean showInReport) {
//     super(owner, "Machine " + id, showInReport);
//     this.owner = owner;
//     this.numCpus = selectCpus + searchCpus;
//     this.hasSelect = selectCpus > 0;

//     if (selectCpus > 0) {
//       // create and queue appropriate number of CSI/merge handling cpus
//       idleSelectThreads = new Queue<>(owner, "Mach " + id + " select threads", showInReport, true);
//       for (int j = 0; j < selectCpus; j++) {
//         JobThread newThread = new JobThread(owner, "Select thread " + j, true, this);
//         idleSelectThreads.insert(newThread);
//       }

//       // create job queue for merge tasks
//       mergeQueue = new Queue<>(owner, "Mach " + id + " merge queue", showInReport, true);

//     } else {
//       idleSelectThreads = null;
//       mergeQueue = null;
//     }

//     if (searchCpus > 0) {
//       idleSearchThreads = new Queue<>(owner, "Mach " + id + " search threads", showInReport, true);
//       for (int i = 0; i < searchCpus; i++) {
//         JobThread newThread = new JobThread(owner, "Search thread " + i, true, this);
//         idleSearchThreads.insert(newThread);
//       }

//       // create job queue for search tasks
//       searchQueue = new Queue<>(owner, "Mach " + id + " search queue", showInReport, true);

//     } else {
//       idleSearchThreads = null;
//       searchQueue = null;
//     }

//     sendMessageQueue = new Queue<>(owner, id + " sendQ", true, true);
//     if (selectCpus > 0) {
//       mergeQueue = new Queue<>(owner, id + " mergeQ", true, true);
//     }
//     if (searchCpus > 0) {
//       searchQueue = new Queue<>(owner, id + " searchQ", true, true);
//     }
//   }

  public boolean hasIdleThread() {
    return idleThreads.size() > 0;
  }

  public void startNewQuery(QueryResourceSelectionTask task) {
    JobThread thread = idleThreads.removeFirst();
    selectingCpus += 1;
    selectLoad.update();

    task.process(thread);
    totalLoad.update();
  }

  public void queueTask(QueryTask task) {
    assert (task.getDestination() == this);
    if (task instanceof QueryMergeTask) {
        // task.setQueuingPriority(200000 - (int) task.getQuery().getArrivalTime());
        mergeQueue.insert((QueryMergeTask) task);
      if (task.getQuery().trackQuery()) {
        mergeQueueTally.update(mergeQueue.size());
      }
    } else if (task instanceof QueryShardSearchTask) {
        if (owner.queueOrder == 4) {
            QueryShardSearchTask curr = searchQueue.last();
            if (curr != null) {
                Double time = ((QueryShardSearchTask) task).time;
                while(curr != null && (curr.expected + time <= curr.getQuery().maxCost)) {
                    curr.expected+=time;
                    curr=searchQueue.pred(curr);
                }
                if (curr == null) searchQueue.insertBefore(((QueryShardSearchTask) task), searchQueue.first());
                else searchQueue.insertAfter(((QueryShardSearchTask) task), curr);
            } else searchQueue.insert((QueryShardSearchTask) task);
        } else {
            if (owner.queueOrder > 0) {
                int cost = (int) (
                    SearchLoadModel.convertCostToTime(((QueryShardSearchTask) task).cost, task.getQuery())
                );
                int priority = owner.queueOrder == 3  ? (int) task.getQuery().maxCost : cost;
                if (owner.queueOrder == 2) priority+=(int) task.getQuery().getSearchArrivalTime();
                // if (owner.queueOrder == 3 && priority == cost) priority=0;
                task.setQueuingPriority(20000-priority);
            }
            searchQueue.insert((QueryShardSearchTask) task);
        }
        if (task.getQuery().trackQuery()) {
            searchQueueTally.update(searchQueue.size());
        }
        searchQueueSizeUpdate(task, true);
    } else {
        throw new RuntimeException("Bad query task class for Machine#queueTask: " + task.getClass());
    }

    // after getting a task, check if there are available resources
    if (!idleThreads.isEmpty()) {
      JobThread thread = idleThreads.removeFirst();
      QueryTask nextTask = prepareNextTask();

      if (nextTask != null) {
        nextTask.process(thread);
      }
    }
    totalLoad.update();
  }

  /**
   * TODO: could replace with a policy class Takes next task to do off of the appropriate queue and
   * returns it. Also updates CPU usage numbers.
   * 
   * @return Task object if there is a task to do. null if there's nothing to do.
   */
  private QueryTask prepareNextTask() {
    QueryTask nextTask = null;
    // merge as priority
    if (hasSelect && !mergeQueue.isEmpty()) {
      // see if there's a merge task for the cpu and if so, process
      nextTask = mergeQueue.removeFirst();
      mergingCpus += 1;
      mergeLoad.update();
    } else if (!searchQueue.isEmpty()) {
      // get next query and process
      nextTask = searchQueue.removeFirst();
      searchingCpus += 1;
      searchLoad.update();
    } else if (hasSelect && !owner.centralQueue.isEmpty()) {
      // no jobs! grab jobs from the central queue and process?
      nextTask = owner.centralQueue.removeFirst();
      selectingCpus += 1;
      selectLoad.update();
    }

    return nextTask;
  }

  public JobThread getIdleThread() {
    return idleThreads.first();
  }

  public void receiveMessage(QueryTask task) {
    assert (task.getDestination() == this);
    queueTask(task);
  }

  /**
   * start the message send
   * 
   * @param task
   */
  public void sendMessage(QueryTask task) {
    // find destination of message
    Machine dest = task.getDestination();
    if (dest == null) {
      throw new RuntimeException("Destination is null: " + task.getName());
    }

    // if the location of this task to be done is local, send self message about its arrival
    if (dest == this) {
      QueryTask.generateArrivalEvent(task).schedule(task, TimeSpan.ZERO);
      // queueTask(task);
      return;
    }

    sendMessageQueue.insert(task);
    if (task.getQuery().trackQuery()) {
      messageQueueTally.update(sendMessageQueue.size());
    }
    
    if (!sendBusy) {
      // then the send queue was free; start sending this message
      QueryTask currTask = sendMessageQueue.removeFirst();
      // MachineEndSendMsgEvent.getInstance().schedule(currTask, Switch.sendTime(currTask));
      new MachineEndSendMsgEvent(owner, "endSend " + currTask.getName(), currTask.traceIsOn())
          .schedule(currTask, Switch.sendTime(currTask));
      sendBusy = true;
    }
    // otherwise, the send is busy right now; exit
  }

  /**
   * Finished sending task to switch.
   * 
   * @param task
   */
  public void sentMessage(QueryTask task) {
    sendBusy = false;
    new SwitchMsgReceivedEvent(owner, "swRecieve " + task.getName(), task.traceIsOn()).schedule(
        networkSwitch, task, Switch.PROPAGATION_DELAY);

    if (!sendMessageQueue.isEmpty()) {
      QueryTask newTask = sendMessageQueue.removeFirst();
      // MachineEndSendMsgEvent.getInstance().schedule(newTask, Switch.sendTime(newTask));
      new MachineEndSendMsgEvent(owner, "endSend " + newTask.getName(), task.traceIsOn()).schedule(
          newTask, Switch.sendTime(newTask));
      sendBusy = true;
    }
  }

  /**
   * 
   * @param task
   *          Task that was using the thread.
   * @param thread
   */
  public void releaseThread(QueryTask task, JobThread thread) {
    if (task instanceof QueryMergeTask) {
      mergingCpus -= 1;
      mergeLoad.update();
    } else if (task instanceof QueryShardSearchTask) {
      searchingCpus -= 1;
      searchLoad.update();
      searchQueueSizeUpdate(task, false);
    } else if (task instanceof QueryResourceSelectionTask) {
      selectingCpus -= 1;
      selectLoad.update();
    } else {
      throw new RuntimeException("Bad query task class for Machine#queueTask: " + task.getClass());
    }

    QueryTask nextTask = prepareNextTask();
    if (nextTask != null) {
      nextTask.process(thread);
    } else {
      // idle the thread
      assert(!idleThreads.contains(thread));
      idleThreads.insert(thread);
    }
    totalLoad.update();
  }

  public int getNumCpus() {
    return numCpus;
  }

  public Switch getSwitch() {
    return networkSwitch;
  }
  
  public boolean searchOverloaded() {

    if (searchQueue.size() > prevSearchQ) {
      searchGrowth++;
      prevSearchQ = searchQueue.size();
    } else if (searchGrowth > 0 && searchQueue.size() < prevSearchQ*0.9) {
      searchGrowth = 0;
      prevSearchQ = SearchLoadModel.QUEUE_BACKUP_MIN;
    }
    
    if (searchGrowth > SearchLoadModel.QUEUE_GROWTH_LIMIT) {
      return true;
    }
    return false;
  }
  
  public boolean mergeOverloaded() {
    if (!hasSelect) {
      return false;
    }
    
    if (mergeQueue.size() > prevMergeQ) {
      mergeGrowth++;
      prevMergeQ = mergeQueue.size();
    } else if (mergeQueue.size() < prevMergeQ) {
      mergeGrowth = 0;
      prevMergeQ = SearchLoadModel.QUEUE_BACKUP_MIN*2;
    }
    
    if (mergeGrowth > SearchLoadModel.QUEUE_GROWTH_LIMIT) {
      return true;
    }
    return false;
  }

  public void searchQueueSizeUpdate(QueryTask task, Boolean add) {
      double taskCost = SearchLoadModel.convertCostToTime(
          ((QueryShardSearchTask) task).cost, task.getQuery()
      );
      if (add) this.machineQueueSize+=taskCost;
      else this.machineQueueSize-=taskCost;
  }
  // create utilization value supplier class to track utilization of machines
  // class CpuUtilizationSupplier extends ValueSupplier {
  // int cpu;
  // public CpuUtilizationSupplier(String name, int cpu) {
  // super(name);
  // this.cpu = cpu;
  // }
  // @Override
  // public double value() {
  // return 1-idleCpuThreadQueues.get(cpu).size()/(double)shardNumThreads.get(cpu);
  // }
  // }
}
