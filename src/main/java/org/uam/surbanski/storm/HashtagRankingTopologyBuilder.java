package org.uam.surbanski.storm;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.uam.surbanski.storm.bolt.HashtagExtractorBolt;
import org.uam.surbanski.storm.bolt.RollingCountBolt;
import org.uam.surbanski.storm.spout.TwitterSpout;

public class HashtagRankingTopologyBuilder {
    static StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-spout", new TwitterSpout());
        builder.setBolt("hashtag-extractor", new HashtagExtractorBolt()).shuffleGrouping("twitter-spout");
        builder.setBolt("hashtag-counter", new RollingCountBolt()).fieldsGrouping("hashtag-extractor", new Fields("hashtag"));

        return builder.createTopology();
    }
}
