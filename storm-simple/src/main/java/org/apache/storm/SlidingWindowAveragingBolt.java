package org.apache.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

public class SlidingWindowAveragingBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        if (!tuples.isEmpty()) {
            double average = tuples.stream()
                    .mapToInt(tuple -> tuple.getIntegerByField("odd-number"))
                    .average()
                    .orElse(0);
            System.out.println("Sliding window average: " + average);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields as it just logs the average
    }
}
