package stormTP.operator;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class SpeedBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow window) {
        // traitement sur fenêtre
        // ...
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(true, new Fields("id", "speed"));
    }
}
