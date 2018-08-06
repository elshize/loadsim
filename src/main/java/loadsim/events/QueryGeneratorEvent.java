package loadsim.events;

import java.util.Arrays;
import java.util.Scanner;

import loadsim.Query;
import loadsim.QueryResourceSelectionTask;
import loadsim.QueryShardSearchTask;
import loadsim.SearchLoadModel;
import desmoj.core.simulator.ExternalEvent;
import desmoj.core.simulator.TimeSpan;

/**
 * Generates arriving query events given a list of queries to generate.
 * 
 * @author yubink
 * 
 */
public class QueryGeneratorEvent extends ExternalEvent {

  private static final int[] exhaustive_docs = {1000, 569, 313, 176, 102, 61, 38, 25, 17, 13, 10};
  
  private SearchLoadModel owner;
  private Scanner queryListFile;
  public boolean finished = false;
  public Query lastQuery;

  public QueryGeneratorEvent(SearchLoadModel owner, String name, boolean showInTrace,
      Scanner queryListFile) {
    super(owner, name, showInTrace);
    this.owner = owner;
    this.queryListFile = queryListFile;
  }
  
  private Query getNextQuery() {
    // get first line and instantiate query; first line is queryNum selectionCost queryTerm1 qTerm2
    String[] linearr = queryListFile.nextLine().split(" ");
    int currQueryNum = Integer.parseInt(linearr[0]);
    int selectionCost = (int)Double.parseDouble(linearr[1]);
    String[] queryTerms = Arrays.copyOfRange(linearr, 2, linearr.length);
    
    Query currQuery = new Query(owner, currQueryNum, true, queryTerms, selectionCost);
    
    int totDocs = 0;
    double maxCost = 0;
    double minCost = 100000000;
    // read in shard search costs
    while (queryListFile.hasNext()) {
      String line = queryListFile.nextLine();
      
      // empty line indicates start of next query
      if (line.isEmpty()) {
       break;
      }
      
      // other lines are shardId searchCost pairs 
      linearr = line.split(" ");
      String shard = linearr[0];
      QueryShardSearchTask shardSearch = new QueryShardSearchTask(owner, currQuery.getName() + " shard "
          + shard, true, currQuery, shard, Long.parseLong(linearr[1]));
      currQuery.searchTasks.add(shardSearch);
      
      maxCost = Math.max(SearchLoadModel.convertCostToTime(shardSearch.cost, currQuery), maxCost);
      minCost = Math.min(SearchLoadModel.convertCostToTime(shardSearch.cost, currQuery), minCost);

      // count (roughly) the documents returned from each shard (1k max) for merge cost estimation
      totDocs += Math.min(1000, shardSearch.cost);
    }
    
    if (owner.exhaustive) {
      // if it's exhaustive search, return less documents per shard
      int idx = (int)(Math.log(currQuery.searchTasks.size())/Math.log(2));
      totDocs = exhaustive_docs[idx]*currQuery.searchTasks.size();
    }
    
    // if more than one shards are search, set a merge cost
    if (currQuery.searchTasks.size() > 1) {
        currQuery.setMergeCost(totDocs * 0.4 / 8000);
        // currQuery.setMergeCost(0);
    }
    currQuery.maxCost = maxCost;
    currQuery.minCost = minCost;
    return currQuery;
  }

  @Override
  public void eventRoutine() {
    // if the file is done, just return
    if (!queryListFile.hasNext()) {
      finished = true;
      return;
    }
        
    Query currQuery = getNextQuery();
    lastQuery = currQuery;
    
    QueryResourceSelectionTask task = new QueryResourceSelectionTask(owner, currQuery.getName()
        + " resource sel task", true, currQuery);
    
    // generate next query arrival event
    CentralQueryTaskArrivalEvent event = new CentralQueryTaskArrivalEvent(owner, "query "+currQuery.getQueryNum(), true);
    event.schedule(task, TimeSpan.ZERO);    

    // schedule self to generate another query after appropriate time
    schedule(owner.getQueryArrivalTime());
  }
}
