package stormTP.operator;


import java.util.Map;
import java.util.HashMap;
//import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.stream.StreamEmiter;

public class MyTortoiseBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final int watchedId;
    private final String name;
    private int lastCell = -1;
    private int lastTop = -1;
    private int totalCells = 0;
    private int maxcel = 0;

    public MyTortoiseBolt(int watchedId, String name) {
        this.watchedId = watchedId;
        this.name = name;
    }

    public MyTortoiseBolt() {
        this.watchedId = -1;
        this.name = "unknown";
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int id = input.getIntegerByField("id");
        if (id != watchedId) {
            collector.ack(input);
            return; // filter out
        }
        int top = input.getIntegerByField("top");
        int cellule = input.getIntegerByField("cellule");
        int total = input.getIntegerByField("total");
        maxcel = input.getIntegerByField("maxcel");

        if (lastTop == -1) {
            // first observation
            lastCell = cellule;
            lastTop = top;
        } else {
            // compute distance taking into account circular track
            int delta = cellule - lastCell;
            if (delta < 0) delta += maxcel; // moved past 0
            totalCells += delta;
            lastCell = cellule;
            lastTop = top;
        }


        collector.emit(new Values(id, top, name, totalCells, total, maxcel));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "nom", "nbCellsParcourus", "total", "maxcel"));
    }
}