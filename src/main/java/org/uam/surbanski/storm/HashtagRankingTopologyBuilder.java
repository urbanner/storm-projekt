package org.uam.surbanski.storm;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.uam.surbanski.storm.spout.TwitterSpout;

public class HashtagRankingTopologyBuilder {
    public static StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-spout", new TwitterSpout());

        return builder.createTopology();
    }
}
