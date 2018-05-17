package org.uam.surbanski.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;

public class RemoteTopologyRunner {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormTopology stormTopology = HashtagRankingTopologyBuilder.build();

        Config config = new Config();
        //config.setNumWorkers(2);
        config.setMessageTimeoutSecs(120);

        StormSubmitter.submitTopology("twitter-hashtag-ranking-topology", config, stormTopology);
    }
}
