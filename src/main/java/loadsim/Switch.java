package loadsim;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import loadsim.events.SwitchEndSendMsgEvent;
import desmoj.core.simulator.Entity;
import desmoj.core.simulator.Queue;
import desmoj.core.simulator.TimeInstant;
import desmoj.core.simulator.TimeSpan;

public class Switch extends Entity {
  
  // UDP (8 bytes) + IP (20 bytes) + Ethernet (26 bytes) + Program header (1 byte)
  public static final int MESSAGE_HEADER = 55;
  public static final int MTU = 1500;
  
  public static final TimeSpan PROPAGATION_DELAY = TimeSpan.ZERO;

  private SearchLoadModel model;
  private Map<Machine, Queue<QueryTask>> sendQueues = new HashMap<Machine, Queue<QueryTask>>();
  private Map<Machine, Boolean> sendBusy = new HashMap<>();

  // tracks the business of the "send" wires
  Map<Integer, TimeInstant> busyMap = new HashMap<Integer, TimeInstant>();
  
  public Switch(SearchLoadModel model, int numMach) {
    super(model, "Switch", true);
    this.model = model;
  }

  /**
   * 
   * @param task Task to be sent
   */
  public void sendMessage(QueryTask task) {
    Machine mach = task.getDestination();
    Queue<QueryTask> sendQueue = sendQueues.get(mach);
    sendQueue.insert(task);
    
    if (!sendBusy.get(mach)) {
      QueryTask currTask = sendQueue.removeFirst();
//      SwitchEndSendMsgEvent.getInstance().schedule(currTask, Switch.sendTime(currTask));
      new SwitchEndSendMsgEvent(model, "swEndSend", traceIsOn()).schedule(currTask, Switch.sendTime(currTask));
      sendBusy.put(mach, true);
    }
  }
  
  public void sentMessage(QueryTask task) {
    Machine mach = task.getDestination();
    Queue<QueryTask> sendQueue = sendQueues.get(mach);
    sendBusy.put(mach, false);
    QueryTask.generateArrivalEvent(task).schedule(task, PROPAGATION_DELAY);
    
    if (!sendQueue.isEmpty()) {
      QueryTask currTask = sendQueue.removeFirst();
//      SwitchEndSendMsgEvent.getInstance().schedule(currTask, Switch.sendTime(currTask));
      new SwitchEndSendMsgEvent(model, "swEndSend", traceIsOn()).schedule(currTask, Switch.sendTime(currTask));
      sendBusy.put(mach, true);
    }
  }
  
  /**
   * Calculates the network transfer delay of a 1 Gbps FastEthernet given the message size in bytes.
   * Based on Cacheda 2007 IP&M
   * 
   * @param messageSize Size of message to be delivered in bytes.
   * @return Delay for network cost.
   */
  public static TimeSpan sendTime(QueryTask task) {
    int messageSize;
    if (task instanceof QueryMergeTask) {
      // query string + results 
      messageSize = task.getQuery().getQueryByteSize();
      
      // number of documents (docid, score)
      messageSize += Math.min(1000, ((QueryMergeTask)task).getShardTask().cost) * 8;
      
    } else if (task instanceof QueryShardSearchTask) {
      // query string + integer to say which shard to search
      messageSize = task.getQuery().getQueryByteSize() + 4;
      
    } else if (task instanceof QueryResourceSelectionTask) {
      // just the query string
      messageSize = task.getQuery().getQueryByteSize();
    } else {
      throw new RuntimeException("Bad query task class for Machine#queueTask: " + task.getClass());
    }
    
    int parts = messageSize / (MTU - MESSAGE_HEADER);
    int totalBytes = (parts+1)*MESSAGE_HEADER + messageSize;
    // return new TimeSpan(0, TimeUnit.MILLISECONDS);
    return new TimeSpan(totalBytes / 125000.0, TimeUnit.MILLISECONDS);
  }

  public void addMachine(Machine mach) {
    sendQueues.put(mach, new Queue<QueryTask>(model, mach.id + " switchQ", true, traceIsOn()));
    sendBusy.put(mach, false);
  }
  
  /**
   * Checks if the switch send queues are empty; for model end condition checking.
   * @return
   */
  public boolean queuesEmpty() {
    for (Queue<QueryTask> q : sendQueues.values()) {
      if (!q.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public boolean isOverloaded() {
    return false;
  }
}
