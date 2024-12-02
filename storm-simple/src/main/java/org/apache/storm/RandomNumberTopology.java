package org.apache.storm;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class RandomNumberTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // Spout configuration
        int spoutExecutors = 2;
        int spoutTasks = 4;
        builder.setSpout("random-spout", new RandomNumberSpout(), spoutExecutors)
                .setNumTasks(spoutTasks);

        // Add the Filtering Bolt
        int filterBoltExecutors = 2;
        builder.setBolt("filter-odd-bolt", new FilterOddBolt(), filterBoltExecutors)
                .shuffleGrouping("random-spout");

        // Add the Averaging Bolt
        int averagingBoltExecutors = 1;
        builder.setBolt("averaging-bolt", new AveragingBolt(), averagingBoltExecutors)
                .shuffleGrouping("filter-odd-bolt");

        // Add the Sliding Window Averaging Bolt
        builder.setBolt("sliding-window-averaging-bolt", new SlidingWindowAveragingBolt()
                        .withWindow(BaseWindowedBolt.Count.of(10), BaseWindowedBolt.Count.of(5)), 1)
                .shuffleGrouping("filter-odd-bolt");

        // Config settings
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(2);
        config.setMessageTimeoutSecs(30);

        // Submit the topology to the cluster
        StormSubmitter.submitTopology("RandomNumberTopology", config, builder.createTopology());
    }
}
