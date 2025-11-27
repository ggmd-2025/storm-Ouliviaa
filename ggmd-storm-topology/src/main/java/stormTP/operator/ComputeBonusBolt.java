package stormTP.operator;



import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.stream.StreamEmiter;
import java.util.concurrent.ConcurrentHashMap;

public class ComputeBonusBolt extends BaseRichBolt {
    private OutputCollector collector;
    // store cumulative points per id
    private final Map<Integer, Integer> cumulative = new ConcurrentHashMap<>();
    // store last processed top per id to build windows of 15 tops
    private final Map<Integer, Integer> lastWindowStart = new ConcurrentHashMap<>();


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void execute(Tuple input) {
        // expects tuples: (id, top, nom, rang, total, maxcel)
        int id = input.getIntegerByField("id");
        int top = input.getIntegerByField("top");
        String rang = input.getStringByField("rang");
        int total = input.getIntegerByField("total");


        // determine window of 15 tops
        int windowStart = lastWindowStart.getOrDefault(id, top);
        // if we've reached a boundary (every 15 tops), compute bonus
        if (top - windowStart + 1 >= 15) {
            // parse rank number (strip 'ex' if present)
            int r = Integer.parseInt(rang.replaceAll("ex$", ""));
            int points = total - r; // as per spec
            cumulative.merge(id, points, Integer::sum);
            String topsStr = windowStart + "-" + (windowStart + 14);
            collector.emit(new Values(id, topsStr, cumulative.get(id)));
            // start new window after this
            lastWindowStart.put(id, windowStart + 15);
        }
        collector.ack(input);
    }


    @Override
    public void declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "tops", "points"));
    }
}