package loadsim;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math.stat.descriptive.rank.Median;

import loadsim.events.QueryGeneratorEvent;
import loadsim.utils.CSVWriter;
import loadsim.utils.CorrelationTally;
import desmoj.core.dist.ContDistExponential;
import desmoj.core.simulator.Experiment;
import desmoj.core.simulator.Model;
import desmoj.core.simulator.ModelCondition;
import desmoj.core.simulator.Queue;
import desmoj.core.simulator.TimeInstant;
import desmoj.core.simulator.TimeSpan;
import desmoj.core.statistic.Histogram;
import desmoj.core.statistic.Tally;

/**
 * This simulates a certain number of CPUs to which are assigned certain lists of shards. 
 * Each CPU has some number of threads so it can run query jobs in parallel.
 * @author yubink
 *
 */
public class SearchLoadModel extends Model {
    public static final int QUEUE_BACKUP_MIN = 100;
    public static final int QUEUE_GROWTH_LIMIT = 10;
    public static final int MONITOR_INTERVAL = 100;
    public static double MAX_SLOPE = 0.001; // maximum slope that the fitted regression line of queue length is allowed
    // MAX_SLOPE == 0.001
    /**
   * Number of CPUs (or machines) available
   */
  private int numMachines;
  
  /**
   * Mean of query arrival time
   */
  private double queryArrivalMean;
  /**
   * Random number stream used to draw an arrival time for the next query.
   */
  private ContDistExponential queryArrivalTime;
  /**
   * Pumps query stats out of simulator to csv files 
   */
  public CSVWriter writer;
  /**
   * Type of queuing strategy used 
   */
  public int queueOrder = 0;
    /**
   * Help maintain consistency across different sets of params
   * Or could be a reminder of someones birthday
   */
  public int RANDOM_SEED = 0;
      /**
   * Type of dispatch strategy used
   */
  public int DISPATCH_STRATEGY = 1;
  /**
   * A waiting queue object is used to represent waiting queue for queries.
   * Every time a query arrives, it is inserted here and is removed 
   * and will be removed by the search system for service.
   */
  public Queue<QueryResourceSelectionTask> centralQueue;
  
  public Tally mergeTimes;
  
  // query effective dead-transit time can be calculated by subtracting everything else from 
  // end to end time. 
  
  /**
   * Statistics gatherer to measure query wait in queue time
   */
  public Histogram queryWaitTime;
  
  /**
   * Statistics gatherer to measure time took to select query shards
   */
  public Tally querySelectionTime;
  /**
   * Statistics gatherer to measure query processing time
   */
  public Histogram queryTotalProcTime;
  
  public Tally queryVisibleQueueTime;
  public Tally queryVisibleTransitTime;
  
  // keeps track of the lengths of central queues seen
  public CorrelationTally centralQueueTally;
  
  /**
   * Statistics for end-to-end query wait/processing time 
   */
  public Histogram queryEndToEndTime;
  
  /**
   * List of all query processing times for additional stats not provided by the histogram
   */
  public List<Double> allQueryEndToEndTimes; 
  
  /**
   * Reference to query generation object to check for end condition.
   */
  public QueryGeneratorEvent queryGen;
  /**
   * Mapping between shard ids and machine maps  
   */
  public Map<String, List<String>> shardMap;
  
  /**
   * Mapping between machine id and machines
   */
  public Map<String, Machine> machineMap;
  
  public Switch networkSwitch;

  
  /**
   * List of queries to process; kept for init()
   */
  private Scanner querylist;
  
  private List<Integer> numThreads;
  private List<String> machineIds;
  private int numSelectMachines;
  public List<Machine> selectMachines; // machines used for resource selection 
  
  // number of mirrors?
  private int mirrors;
  /**
   * Way to halt simulation; for example if the waiting queue is too long
   */
  public boolean abort = false;
  public boolean monitorLoad;
  
  // is this experiment for exhaustive search?
  public boolean exhaustive;
  
  // series of variables to keep track of how backed-up the query queue is
  private int prevQueueSize = SearchLoadModel.QUEUE_BACKUP_MIN;
  private int growthCount = 0;
  
  // because everybody needs a random number generator
  private Random rand = new Random();
  
  public SearchLoadModel(Model owner, String name, boolean showInReport, boolean showInTrace,
      List<Integer> numThreads, List<String> machineIds, int mirrors, int numSelectMachines, double queryArrivalMean,
      Map<String, List<String>> shardMap, Scanner querylist, boolean monitorLoad, boolean exhaustive, 
      int queueOrder, int randomSeed, int dispatchStrategy) {
    super(owner, name, showInReport, showInTrace);
    this.numThreads = numThreads;
    this.machineIds = machineIds;
    this.numMachines = numThreads.size();
    this.numSelectMachines = numSelectMachines;
    this.queryArrivalMean = queryArrivalMean;
    this.shardMap = shardMap;
    this.querylist = querylist;
    this.allQueryEndToEndTimes = new ArrayList<>();
    this.machineMap = new HashMap<>();
    this.mirrors = mirrors;
    this.monitorLoad = monitorLoad;
    this.exhaustive = exhaustive;
    this.queueOrder = queueOrder;
    this.writer = new CSVWriter(this);
    this.RANDOM_SEED = randomSeed;
    this.DISPATCH_STRATEGY = dispatchStrategy;
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        public void run() {
            try {
				writer.write();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
        }
    }));
  }

  /*
  public SearchLoadModel(Model owner, String name, boolean showInReport, boolean showInTrace,
      List<Integer> shardNumThreads, List<Integer> csiNumThreads, double queryArrivalMean,
      Map<String, String> shardMap, Scanner querylist) {
    super(owner, name, showInReport, showInTrace);
    this.shardNumThreads = shardNumThreads;
    this.csiNumThreads = csiNumThreads;
    for (Integer num : csiNumThreads) {
      totCsiNumThreads += num;
    }
    this.numMachines = shardNumThreads.size();
    this.queryArrivalMean = queryArrivalMean;
    this.shardMap = shardMap;
    this.querylist = querylist;
    this.allQueryEndToEndTimes = new ArrayList<>();
    this.machineMap = new HashMap<>();
  }
  */

  @Override
  public String description() {
    return "Simulation of query loads for selective search.";
  }

  @Override
  public void doInitialSchedules() {
    queryGen.schedule(new TimeSpan(0));
    if (monitorLoad) {
      //TODO
//      loadMonitor.schedule(new TimeSpan(MONITOR_INTERVAL));
    }
  }

  @Override
  public void init() {
    // set up statistics collectors
    //queryTotalProcTime = new Tally(this, "Query total processing time (incl. shard wait)", true, true);
    // double[] range = {0.0, 5.0, 10.0, 15.0, 50.0, 100.0, 200.0, 1000.0};
    queryWaitTime = new Histogram(this, "Query wait in queue time", 0.0, 400.0, 20, true, false);
    querySelectionTime = new Tally(this, "Query resource selection time (not incl. wait)", true, false);
    queryTotalProcTime = new Histogram(this, "Query processing time (incl. shard wait)",
        0, 1000, 12, true, false);
    queryVisibleTransitTime = new Tally(this, "Query visible transit", true, false);
    queryVisibleQueueTime = new Tally(this, "Query visible queue", true, false);
    centralQueueTally = new CorrelationTally(this, "Central queue sizes", true, true);
    queryEndToEndTime = new Histogram(this, "Query end-to-end time", 0.0, 2000, 10, true, false);
    mergeTimes = new Tally(this, "Query merging costs", true, false);
    
    // set up query generator
    centralQueue = new Queue<QueryResourceSelectionTask>(this, "Central queue", true, true);
    queryArrivalTime = new ContDistExponential(this, "QueryArrivalTime", 
        queryArrivalMean, true, false);
    queryArrivalTime.setNonNegative(true);
    queryArrivalTime.setSeed(RANDOM_SEED*7);
    rand.setSeed(RANDOM_SEED*3);
       
    networkSwitch = new Switch(this, numMachines);
    selectMachines = new ArrayList<Machine>();
    
    // initialize as many mirrors as necessary
    for (int j = 0; j < mirrors; j++) {
      // initialize machines
      for (int i = 0; i < machineIds.size(); i++) {
        String machineId = machineIds.get(i) + "." + j;
        Machine mach;
        if (i < numSelectMachines) {
          mach = new Machine(this, machineId, j, numThreads.get(i), networkSwitch, true, true);
          selectMachines.add(mach);
        } else {
          mach = new Machine(this, machineId, j, numThreads.get(i), networkSwitch, false, true);
        }
        machineMap.put(machineId, mach);
        networkSwitch.addMachine(mach);
      }
    }

    queryGen = new QueryGeneratorEvent(this, "Query Generator", true, querylist);
//    loadMonitor = new LoadMonitorEvent(this, "load monitor", false, MONITOR_INTERVAL, MAX_SLOPE);
  }
  
  public TimeSpan getQueryArrivalTime() {
    return queryArrivalTime.sampleTimeSpan(TimeUnit.MILLISECONDS);
  }
  
  public static TimeSpan convertCostToTimeSpan(int postingCost, Query query) {
    return convertCostToTimeSpan((long)postingCost, query);
  }

  public static TimeSpan convertCostToTimeSpan(long postingCost, Query query) {
    // if there was no postings to retrieve, don't incur disk cost
    if (postingCost == 0) return new TimeSpan(0);
    double millisecs = convertCostToTime(postingCost, query);
    return new TimeSpan(millisecs, TimeUnit.MILLISECONDS);
  }

  public static double convertCostToTime(long postingCost, Query query) {
    // if there was no postings to retrieve, don't incur disk cost
    if (postingCost == 0) return 0.0;
    double millisecs = postingCost/1067.701 + query.getQueryTerms().length*0.4;
    return millisecs;
  }
  
  public static TimeSpan timeDifference(TimeInstant later, TimeInstant earlier) {
    return new TimeSpan(later.getTimeAsDouble(TimeUnit.MILLISECONDS)
        - earlier.getTimeAsDouble(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
  }

  /**
   * Generate a merge cost for this query.
   * 
   * @param query
   * @return
   */
  public static TimeSpan getMergeCost(Query query) {
    return new TimeSpan(query.getMergeCost(), TimeUnit.MILLISECONDS);
  }
  
  /**
   * Given a shard id, looks up the machine it is assigned to returns the machine number.
   * Caller will have to append the mirror suffix to get the actual machine id. 
   * @param shard
   * @return
   */
  public String getMachinePrefix(String shard) {
    List<String> candidates = shardMap.get(shard);
    return candidates.get(rand.nextInt(candidates.size()));
  }

  public List<String> getMachinePrefixList(String shard) {
    List<String> candidates = shardMap.get(shard);
    return candidates;
  }
  
  
  /**
   * @param args
   * @throws FileNotFoundException 
   */
  public static void main(String[] args) throws FileNotFoundException {

    // read in parameter file
    Map<String, String> params = new HashMap<>();
    try (Scanner scan = new Scanner(new File(args[0]))) {
      while (scan.hasNext()) {
        String line = scan.nextLine();
        String[] pair = line.split("=");
        params.put(pair[0], pair[1]);
      }
    }
    
    // read in the number of threads for processing
    String[] numThreadsStr = params.get("numThreads").split(",");
    List<Integer> numThreads = new ArrayList<Integer>(); 
    for (int i = 0; i < numThreadsStr.length; i++) {
      int num = Integer.parseInt(numThreadsStr[i]);
      numThreads.add(num);
    }
    
    // read in machine ids
    String[] idStr = params.get("machineIds").split(",");
    List<String> machineIds = Arrays.asList(idStr);
    
/*    
    // read in the number of threads for shard processing
    String[] numThreadsStr = params.get("shardNumThreads").split(",");
    List<Integer> shardNumThreads = new ArrayList<Integer>(); 
    for (int i = 0; i < numThreadsStr.length; i++) {
      int num = Integer.parseInt(numThreadsStr[i]);
      shardNumThreads.add(num);
    }
    int numCpu = shardNumThreads.size();
    
    // number of threads for the CSI machine
    String[] csiNumThreadsStr = params.get("csiNumThreads").split(",");
    List<Integer> csiNumThreads = new ArrayList<Integer>();
    for (int i = 0; i < csiNumThreadsStr.length; i++) {
      int num = Integer.parseInt(csiNumThreadsStr[i]);
      csiNumThreads.add(num);
    }
*/
    
    // query arrival rate is given in queries per second; convert to interarrival mean in ms
    double queryMu = 1000/Double.parseDouble(params.get("queryArrivalRate"));
    String shardMappingFile = params.get("shardMappingFile");
    String queryListFile = params.get("queryListFile");
    
    int mirrors = 1;
    if (params.get("mirrors") != null) {
      mirrors = Integer.parseInt(params.get("mirrors")); 
    }
    
    int numSelectMachines = Integer.parseInt(params.get("numSelectMachines"));
    
    // read in shard mapping to machine
    Set<String> idLookup = new HashSet<>(machineIds);  
    Map<String, List<String>> shardMap = new HashMap<>();
    
    try (Scanner mapScan = new Scanner(new File(shardMappingFile))) {
      while(mapScan.hasNext()) {
        String line = mapScan.nextLine();
        String[] pair = line.split(" ");
        if (!idLookup.contains(pair[1])) {
          System.err.println("CPU assignment for '" + line + "' exceeds number of CPUs specified");
          throw new ArrayIndexOutOfBoundsException(pair[1]);
        }
        
        List<String> machines = shardMap.get(pair[0]);
        if (machines == null) {
          machines = new ArrayList<>();
        }
        machines.add(pair[1]);
        shardMap.put(pair[0], machines);
      }
    }
    
    boolean monitorLoad = true;
    if (params.get("monitorLoad") != null) {
      monitorLoad = Boolean.parseBoolean(params.get("monitorLoad"));
    }
    
    // open simdata file, which has queries and their costs
    final Scanner queryListScanner = new Scanner(new File(queryListFile), "iso-8859-1");
    
    // create model and experiment
    // null as first parameter because it is the main model and has no mastermodel
    final SearchLoadModel model = new SearchLoadModel(null,
                          "Selective search query load", true, true,
                          numThreads, machineIds, mirrors, numSelectMachines, queryMu, shardMap, 
                          queryListScanner, monitorLoad, 
                          Boolean.parseBoolean(params.get("exhaustiveSearch")), 
                          Integer.parseInt(params.get("queueOrder")), Integer.parseInt(params.get("randomSeed")),
                          Integer.parseInt(params.get("dispatchStrategy")));
    

    Experiment exp = new Experiment(args[1], TimeUnit.MICROSECONDS,
                          TimeUnit.MILLISECONDS, null);
    //TODO: make random
    exp.setSeedGenerator(model.RANDOM_SEED);
    //exp.setSeedGenerator(new Random().nextLong());
    
    model.connectToExperiment(exp);
    // set experiment parameters
    exp.setShowProgressBar(true);  // display a progress bar (or not)    

    exp.stop(new StopCondition(model, "Stop cond", false, queryListScanner));
    exp.tracePeriod(new TimeInstant(0), new TimeInstant(6000, TimeUnit.MILLISECONDS));
//    exp.debugPeriod(new TimeInstant(0), new TimeInstant(50, TimeUnit.SECONDS));   // and debug output
    exp.start();
    exp.report();
    exp.finish();
    
    queryListScanner.close();
    
    if (model.abort) {
      System.err.println("Aborted.");
      //return;
    }
    //*************************************************
    // convert gathered query times into a primitive double array
    System.out.println("Query times");
    double[] primitive = new double[model.allQueryEndToEndTimes.size()];
    int i = 0;
    for (Double val : model.allQueryEndToEndTimes) {
      System.out.println(val);
      primitive[i] = val;
      i++;
    }
    System.out.println();
    
    List<String> sortedIds = new ArrayList<>(model.machineMap.keySet());
    Collections.sort(sortedIds);
    
    Median medClass = new Median();
    double ninetyninth = medClass.evaluate(primitive, 99);
    double median = medClass.evaluate(primitive);
    System.out.printf("%.2f\t%.2f\t", median, ninetyninth);
    for (String id : sortedIds) {
      Machine mach = model.machineMap.get(id);
      double merge = 0;
      if (mach.hasSelect && mach.mergeLoad.getObservations() > 0) {
        merge = mach.mergeLoad.getMean();
      }
      double select = 0;
      if (mach.hasSelect && mach.selectLoad.getObservations() > 0) {
        select = mach.selectLoad.getMean();
      }
      double search = 0;
      if (mach.searchLoad.getObservations() > 0) {
        search = mach.searchLoad.getMean();
      }
      System.out.printf("%.2f\t%.2f\t%.2f\t", merge, select, search);
    }

    System.out.println();
    
    double overall = model.queryEndToEndTime.getMean();
    if (Double.parseDouble(params.get("queryArrivalRate")) > 5000) {
      System.out.println(exp.getSimClock().getTime().getTimeAsDouble());
    }
//    System.err.println(overall);
//    System.err.println(model.queryWaitTime.getMean() + " " + model.queryVisibleTransitTime.getMean()
//        + " " + model.queryVisibleQueueTime.getMean() + " " + model.querySelectionTime.getMean() + " "
//        +model.mergeTimes.getMean());
//    System.err.printf("%.2f\\%%&%.2f\\%%&%.2f\\%%&%.2f\\%%&%.2f\\%%\n", model.queryWaitTime.getMean()/overall*100, model.queryVisibleTransitTime.getMean()/overall*100,
//    model.queryVisibleQueueTime.getMean()/overall*100, model.querySelectionTime.getMean()/overall*100,
//        model.mergeTimes.getMean()*100/overall);
  }

  public void updateQueryStats(Query query) {
    
    if (!query.trackQuery()) {
      return;
    }
    
    double waitTime = query.getSelectBeginTime() - query.getArrivalTime();
    double visibleTransitTime = (query.getSearchArrivalTime() - query.getSelectEndTime()) 
        + (query.getMergeArrivalTime() - query.getSearchEndTime());
    double visibleQueueTime = (query.getSearchBeginTime() - query.getSearchArrivalTime()) 
        + (query.getMergeBeginTime() - query.getMergeArrivalTime());
    
    double selectTime = query.getSelectEndTime() - query.getSelectBeginTime();
//    double searchTime = query.getSearchEndTime() - query.getSearchBeginTime();
    double processingTime = query.getEndTime() - query.getSelectBeginTime();
    double totalTime = query.getEndTime() - query.getArrivalTime();
    
    assert(waitTime >= 0 && visibleTransitTime >= 0 && visibleQueueTime >= 0);

    writer.update(query);
    
    queryWaitTime.update(waitTime);
    querySelectionTime.update(selectTime);
    queryTotalProcTime.update(processingTime);
    queryEndToEndTime.update(totalTime);
    queryVisibleQueueTime.update(visibleQueueTime);
    queryVisibleTransitTime.update(visibleTransitTime);
    allQueryEndToEndTimes.add(totalTime);
    mergeTimes.update(SearchLoadModel.getMergeCost(query).getTimeAsDouble(TimeUnit.MILLISECONDS));
  }
  
  public boolean queryQueueOverLoaded() {     
    if (centralQueue.size() > prevQueueSize) {
      growthCount++;
      prevQueueSize = centralQueue.size();
    } else if (growthCount > 0 && centralQueue.size() < prevQueueSize*0.9) {
      growthCount = 0;
      prevQueueSize = SearchLoadModel.QUEUE_BACKUP_MIN;
    }

    if (growthCount > SearchLoadModel.QUEUE_GROWTH_LIMIT) {
      return true;
    }
    
    return false;
  }
  
}
