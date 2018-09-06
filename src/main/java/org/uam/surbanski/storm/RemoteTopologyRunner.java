package org.uam.surbanski.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;

public class RemoteTopologyRunner {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setNumWorkers(2);
        config.setMaxSpoutPending(5000);

        StormTopology stormTopology = HashtagRankingTopologyBuilder.build();

        StormSubmitter.submitTopology("surbanski-topologia-twitter-hashtag-ranking", config, stormTopology);
    }
}
