package fr.unice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout; // Add this import statement
import org.apache.storm.kafka.spout.KafkaSpoutConfig; // Add this import statement
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

public class KafkaTopology {
  private TopologyBuilder KafkaSpoutTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new KafkaSpout<>(getKafkaSpoutConfig("kafka:9092")), 1);
    return builder;
  }

  protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
    return KafkaSpoutConfig.builder(bootstrapServers, new String[] { "First_topic" })
        .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
        .setRetry(getRetryService())
        .setOffsetCommitPeriodMs(10000)
        .setFirstPollOffsetStrategy(EARLIEST)
        .setMaxUncommittedOffsets(250)
        .build();
  }

  protected KafkaSpoutRetryService getRetryService() {
    return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
        TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
  }

  public static void main(String[] args) throws Exception {
    KafkaTopology firstTopology = new KafkaTopology();
    TopologyBuilder builder = firstTopology.KafkaSpoutTopology();
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());
      Thread.sleep(10000);
      cluster.shutdown();
    }
  }
}
