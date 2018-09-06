package org.uam.surbanski.storm.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

public class HashtagExtractorBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag", "coordinates"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Status status = (Status) tuple.getValueByField("tweet");

        String[] words = status.getText().trim().split("\\s+");
        String coordinates = status.getUser().getLocation();

        for (String word : words) {
            if (StringUtils.startsWith(word, "#")) {
                if (coordinates != null) {
                    outputCollector.emit(tuple, new Values(word, coordinates));
                }
                else {
                    outputCollector.emit(tuple, new Values(word, "nieudostepnione"));
                }
                outputCollector.ack(tuple);
            }
        }
    }
}
