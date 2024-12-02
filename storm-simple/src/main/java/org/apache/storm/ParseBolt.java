package org.apache.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String logLine = input.getString(0);
        // Simple regex pattern for SSH log parsing
        Pattern pattern = Pattern.compile(".*Failed password for(?: invalid user)? (\\S+) from (\\S+).*");
        Matcher matcher = pattern.matcher(logLine);

        if (matcher.find()) {
            String username = matcher.group(1);
            String ip = matcher.group(2);
            collector.emit(new Values(username, ip, System.currentTimeMillis()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("username", "ip", "timestamp"));
    }
}

