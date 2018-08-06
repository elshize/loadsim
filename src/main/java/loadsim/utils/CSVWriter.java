package loadsim.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Array;
import java.util.ArrayList;

import loadsim.Query;
import loadsim.QueryShardSearchTask;
import loadsim.SearchLoadModel;


public class CSVWriter {
    public ArrayList<Query> querys;
    public ArrayList<QueryShardSearchTask> tasks;
    public String filename;
    public CSVWriter(SearchLoadModel owner) {
        this.querys = new ArrayList<>();
        this.tasks = new ArrayList<>();
        switch(Integer.valueOf(owner.queueOrder)) {
            case 1:
                this.filename = "queue_sc.csv";
                break;
            case 2:
                this.filename = "queue_nd.csv";
                break;
            case 3:
                this.filename = "queue_mnd.csv";
                break;
            default:
                this.filename = "queue_st.csv";
        }
    }
    public void update(Query query) {
        this.querys.add(query);
    }
    public void update(QueryShardSearchTask task) {
        this.tasks.add(task);
    }
    public void write() throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(new File(filename));
        StringBuilder sb = new StringBuilder();
        sb.append("queryNum-0");
        sb.append(',');
        sb.append("queryArrival-1");
        sb.append(',');
        sb.append("searchArrival-2");
        sb.append(',');
        sb.append("searchBegin-3");
        sb.append(',');
        sb.append("searchEnd-4");
        sb.append(',');
        sb.append("mergeArrival-5");
        sb.append(',');
        sb.append("mergeBegin-6");
        sb.append(',');
        sb.append("queryEnd-7");
        sb.append('\n');

        for(Query q: this.querys) {
            sb.append(q.getQueryNum());
            sb.append(',');
            sb.append(q.getArrivalTime());
            sb.append(',');
            sb.append(q.getSearchArrivalTime());
            sb.append(',');
            sb.append(q.getSearchBeginTime());
            sb.append(',');
            sb.append(q.getSearchEndTime());
            sb.append(',');
            sb.append(q.getMergeArrivalTime());
            sb.append(',');
            sb.append(q.getMergeBeginTime());
            sb.append(',');
            sb.append(q.getEndTime());
            sb.append('\n');
        }

        pw.write(sb.toString());
        pw.close();
        writeShards();
        System.out.println("Data written to "+filename);
    }
    public void writeShards() throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(new File("shards_"+filename));
        StringBuilder sb = new StringBuilder();
        sb.append("shardArrival-0");
        sb.append(',');
        sb.append("shardStart-1");
        sb.append(',');
        sb.append("shardFinish-2");
        sb.append(',');
        sb.append("isLast-3");
        sb.append('\n');

        int queryNum = 100000000;
        for(QueryShardSearchTask t: this.tasks) {
            Query query = t.getQuery();
            if (queryNum != query.getQueryNum()) {
                sb.append('\n');
                queryNum = query.getQueryNum();
            }
            sb.append(t.shardArrivalTime);
            sb.append(',');
            sb.append(t.shardStartTime);
            sb.append(',');
            sb.append(t.shardFinishTime);
            sb.append(',');
            sb.append(t.shardFinishTime == query.getSearchEndTime() ? '1' : '0');
            sb.append('\n');
        }
        sb.append('\n');

        pw.write(sb.toString());
        pw.close();
        System.out.println("Data written to shards_"+filename);
    }
}