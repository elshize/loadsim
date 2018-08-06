package loadsim;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import desmoj.core.simulator.Entity;

public class Query extends Entity {
  
  private SearchLoadModel owner;
  protected int selectionCost;
  protected double mergeCost = 0.0; // in milliseconds
  protected int queryNum;
  protected String[] queryTerms;
  public List<QueryShardSearchTask> searchTasks = new ArrayList<>();
  public double maxCost = 0;
  public double minCost = 0;
  // number of bytes in the query string
  protected int queryByteSize;

  private Machine mergeMachine;
  // query state variables follow:
  
  /**
   * Time when query was queued
   */
  private double arrivalTime = -1;
  
  /**
   * Time when query started being processed
   */
  private double selectBeginTime = -1;
  private double selectEndTime = -1;
  private double searchArrivalTime = -1;
  private double searchBeginTime = -1;
  private double searchEndTime = -1;
  private double mergeArrivalTime = -1;
  private double mergeBeginTime = -1;
  private double endTime = -1;

  private QueryState state;

  public Query(SearchLoadModel owner, int queryNum, boolean showInTrace, String[] queryTerms,
      int selectionCost) {
    super(owner, "Query " + queryNum, showInTrace);
    this.owner = owner;
    this.queryNum = queryNum;
    this.queryTerms = queryTerms;
    this.selectionCost = selectionCost;
    
    this.queryByteSize = 0;
    for (String term : queryTerms) {
      this.queryByteSize += 8*term.length();
    }
  }
  
  public int getQueryByteSize() {
    return queryByteSize;
  }
  
  public void setMergeCost(double cost) {
    this.mergeCost = cost;
  }

  // returns merge cost in milliseconds
  public double getMergeCost() {
    return mergeCost;
  }
  
  public int getSelectionCost() {
    return selectionCost;
  }

  public void setSelectionCost(int selectionCost) {
    this.selectionCost = selectionCost;
  }

  public int getQueryNum() {
    return queryNum;
  }
  
  public String[] getQueryTerms() {
    return queryTerms;
  }

  public QueryState getState() {
    return state;
  }
  
  public void setMergeMachine(Machine machine) {
    this.mergeMachine = machine;
  }
  
  public Machine getMergeMachine() {
    return mergeMachine;
  }

  public void updateState(QueryState state) {

    switch (state) {
    case QUEUED:
      sendTraceNote(getName() + " State: QUEUED");
      arrivalTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      break;
    case STARTED:
      sendTraceNote(getName() + " State: STARTED");
      selectBeginTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      break;
    case RESOURCE_SELECTED:
      sendTraceNote(getName() + " State: RESOURCE_SELECTED");
      selectEndTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      break;
    case SHARD_SEARCH_ARRIVAL:
      // record earliest shard search arrival time
      if (searchArrivalTime == -1) {
        sendTraceNote(getName() + " State: SHARD_SEARCH_ARRIVAL");
        searchArrivalTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      }
      break;
    case SHARD_SEARCH_STARTED:
      // record earliest
      if (searchBeginTime == -1) {
        sendTraceNote(getName() + " State: SHARD_SEARCH_STARTED");
        searchBeginTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      }
      break;
    case SHARD_SEARCH_DONE:
      // record lastest time
      sendTraceNote(getName() + " State: SHARD_SEARCH_DONE");
      searchEndTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      break;
    case MERGE_ARRIVAL:
      // record latest time
      sendTraceNote(getName() + " State: MERGE_ARRIVAL");
      mergeArrivalTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      break;
    case MERGE_STARTED:
      sendTraceNote(getName() + " State: MERGE_STARTED");
      mergeBeginTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      break;
    case DONE:
      sendTraceNote(getName() + " State: DONE");
      endTime = owner.presentTime().getTimeAsDouble(TimeUnit.MILLISECONDS);
      // report relevant numbers to model
      owner.updateQueryStats(this);
      break;
    }
    
    if (state != QueryState.SHARD_SEARCH_DONE) {
      this.state = state;
    } else if (allShardsSearched()) {
      this.state = QueryState.SHARD_SEARCH_DONE;
    }
  }

  /**
   * Determines whether all shards have been searched
   * @return
   */
  public boolean allShardsSearched() {
    for (QueryShardSearchTask shard : searchTasks) {
      if (!shard.done) {
        return false;
      }
    }
    return true;
  }
  
  public boolean isDone() {
    return state == QueryState.DONE;
  }
  
  public double getArrivalTime() {
    return arrivalTime;
  }

  public double getSelectBeginTime() {
    return selectBeginTime;
  }

  public double getSelectEndTime() {
    return selectEndTime;
  }

  public double getSearchArrivalTime() {
    return searchArrivalTime;
  }

  public double getSearchBeginTime() {
    return searchBeginTime;
  }

  public double getSearchEndTime() {
    return searchEndTime;
  }

  public double getMergeArrivalTime() {
    return mergeArrivalTime;
  }

  public double getMergeBeginTime() {
    return mergeBeginTime;
  }

  public double getEndTime() {
    return endTime;
  }

  
  public enum QueryState {
    QUEUED, STARTED, RESOURCE_SELECTED, SHARD_SEARCH_ARRIVAL, SHARD_SEARCH_STARTED, SHARD_SEARCH_DONE, MERGE_ARRIVAL, MERGE_STARTED, DONE; 
  }

  /**
   * returns whether or not to collect statistics for this query; i.e. is it not a warm-up query?
   * @return
   */
  public boolean trackQuery() {
    return queryNum >= 0;
  }
  
}
