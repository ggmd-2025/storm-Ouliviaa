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

public class Exit5Bolt extends BaseRichBolt {
    private static final long serialVersionUID = 4262369370788107342L;
	//private static Logger logger = Logger.getLogger("ExitBolt");
	private OutputCollector collector;
	int port = -1;
	StreamEmiter semit = null;

    public Exit5Bolt (int port) {
		this.port = port;
		this.semit = new StreamEmiter(this.port);
		
	}

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Map<String, Object> out = new HashMap<>();
        out.put("id", input.getIntegerByField("id"));
        out.put("nom", input.getStringByField("nom"));
        out.put("tops", input.getStringByField("tops"));
        out.put("vitesse", input.getDoubleByField("vitesse"));
        collector.emit(new Values(out.toString()));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}