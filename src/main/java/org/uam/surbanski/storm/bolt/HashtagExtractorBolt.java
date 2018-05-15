package org.uam.surbanski.storm.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.GeoLocation;
import twitter4j.Status;

public class HashtagExtractorBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Status status = (Status) tuple.getValueByField("tweet");

        String[] words = status.getText().trim().split("\\s+");
        GeoLocation coordinates = status.getGeoLocation();

        for (String word : words) {
            if (StringUtils.startsWith(word, "#")) {
                if (coordinates != null) basicOutputCollector.emit(new Values(word, coordinates.toString()));
                else basicOutputCollector.emit(new Values(word, ""));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag", "coordinates"));
    }
}
