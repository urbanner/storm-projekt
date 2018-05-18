package org.uam.surbanski.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

public class LocalTopologyRunner {
    public static void main(String[] args) {
        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = HashtagRankingTopologyBuilder.build();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("surbanski-topologia-twitter-hashtag-ranking", config, topology);
    }
}
