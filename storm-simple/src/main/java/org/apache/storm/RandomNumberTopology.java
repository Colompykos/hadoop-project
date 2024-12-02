package org.apache.storm;

import org.apache.storm.topology.TopologyBuilder;

public class RandomNumberTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // Add spout with configurable parallelism
        int numExecutors = 2;  // Number of executors
        int numTasks = 4;      // Number of tasks

        builder.setSpout("random-spout", new RandomNumberSpout(), numExecutors)
                .setNumTasks(numTasks);

        Config config = new Config();
        config.setDebug(true);

        // Set the number of workers
        config.setNumWorkers(2);

        // Set message timeout
        config.setMessageTimeoutSecs(30);

        StormSubmitter.submitTopology("RandomNumberTopology", config, builder.createTopology());
    }
}