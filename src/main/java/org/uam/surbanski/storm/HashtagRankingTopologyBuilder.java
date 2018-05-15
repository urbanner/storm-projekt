package org.uam.surbanski.storm;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.uam.surbanski.storm.bolt.HashtagExtractorBolt;
import org.uam.surbanski.storm.spout.TwitterSpout;

public class HashtagRankingTopologyBuilder {
    public static StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-spout", new TwitterSpout());
        builder.setBolt("hashtag-extractor", new HashtagExtractorBolt()).shuffleGrouping("twitter-spout");

        return builder.createTopology();
    }
}
