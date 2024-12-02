package org.apache.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomNumberSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;
    private String componentId;
    private int taskId;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
        this.componentId = context.getThisComponentId();
        this.taskId = context.getThisTaskId();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        int number = random.nextInt(100);
        collector.emit(new Values(number));
        System.out.printf("Task %d (Component: %s) emitting number: %d%n",
                taskId, componentId, number);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }
}