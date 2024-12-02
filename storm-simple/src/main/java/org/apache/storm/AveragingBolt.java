package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.Map;

public class AveragingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private LinkedList<Integer> values;
    private static final int WINDOW_SIZE = 10;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.values = new LinkedList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        int number = tuple.getIntegerByField("odd-number");
        values.add(number);

        if (values.size() > WINDOW_SIZE) {
            values.poll();  // Remove the oldest value if the window exceeds size
        }

        if (values.size() == WINDOW_SIZE) {
            double average = values.stream().mapToInt(Integer::intValue).average().orElse(0);
            System.out.println("Average of last 10 values: " + average);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // This bolt doesn't emit any tuples
    }
}

