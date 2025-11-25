package stormTP.operator;

import java.util.Map;
import java.util.HashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Calcule le rang des tortues à partir de leur position circulaire
 *
 * Entrée : (id, top, tour, cellule, total, maxcel)
 * Sortie : (id, top, rang, total, maxcel)
 */
public class GiveRankBolt extends BaseRichBolt {

    private OutputCollector collector;

    // stocke la distance totale parcourue par chaque tortue
    private Map<Integer, Integer> distances = new HashMap<>();

    // stocke le dernier top pour éviter les duplications
    private Map<Integer, Integer> lastTop = new HashMap<>();

    private int maxcel = 0;
    private int total = 0;

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        int id = input.getIntegerByField("id");
        int top = input.getIntegerByField("top");
        int tour = input.getIntegerByField("tour");
        int cellule = input.getIntegerByField("cellule");
        total = input.getIntegerByField("total");
        maxcel = input.getIntegerByField("maxcel");

        // distance totale = tours * maxcel + cellule
        int distance = tour * maxcel + cellule;

        distances.put(id, distance);
        lastTop.put(id, top);

        // calcul du classement
        Map<Integer,Integer> sorted = sortByDistance(distances);

        int rank = 1;
        int previousValue = -1;
        Map<Integer,String> rankStr = new HashMap<>();

        for (Map.Entry<Integer,Integer> e : sorted.entrySet()) {
            if (previousValue == -1) {
                // premier
                rankStr.put(e.getKey(), "1");
                previousValue = e.getValue();
            } else {
                if (e.getValue() == previousValue)
                    rankStr.put(e.getKey(), rank + "ex");
                else {
                    rank++;
                    rankStr.put(e.getKey(), String.valueOf(rank));
                    previousValue = e.getValue();
                }
            }
        }

        // émission du résultat pour cette tortue
        collector.emit(new Values(id, top, rankStr.get(id), total, maxcel));
        collector.ack(input);
    }

    /** Trie les tortues par distance décroissante */
    private Map<Integer,Integer> sortByDistance(Map<Integer,Integer> map) {
        return map.entrySet().stream()
                .sorted((a,b) -> b.getValue() - a.getValue())
                .collect(HashMap::new,
                         (m,e) -> m.put(e.getKey(), e.getValue()),
                         HashMap::putAll);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "rang", "total", "maxcel"));
    }
}
