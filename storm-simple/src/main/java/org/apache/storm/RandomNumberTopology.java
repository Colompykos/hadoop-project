package org.apache.storm;


import org.apache.storm.topology.TopologyBuilder;

public class RandomNumberTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // Spout configuration
        int spoutExecutors = 2;
        int spoutTasks = 4;
        builder.setSpout("random-spout", new RandomNumberSpout(), spoutExecutors)
                .setNumTasks(spoutTasks);

        // Add the Filtering Bolt
        int filterBoltExecutors = 2;  // Set parallelism for the bolt
        builder.setBolt("filter-odd-bolt", new FilterOddBolt(), filterBoltExecutors)
                .shuffleGrouping("random-spout");

        // Config settings
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(2); // Number of workers
        config.setMessageTimeoutSecs(30); // Message timeout

        // Submit the topology to the cluster
        StormSubmitter.submitTopology("RandomNumberTopology", config, builder.createTopology());
    }
}
