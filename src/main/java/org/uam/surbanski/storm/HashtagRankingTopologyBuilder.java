package org.uam.surbanski.storm;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.uam.surbanski.storm.bolt.HashtagExtractorBolt;
import org.uam.surbanski.storm.bolt.IntermediateRankingsBolt;
import org.uam.surbanski.storm.bolt.RollingCountBolt;
import org.uam.surbanski.storm.bolt.TotalRankingsBolt;
import org.uam.surbanski.storm.spout.TwitterSpout;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

class HashtagRankingTopologyBuilder {

    private HashtagRankingTopologyBuilder() {
        throw new IllegalStateException("Utility class");
    }

    static StormTopology build() {

        // implementacja KafkaBolta
        Properties props = new Properties();
        props.put("bootstrap.servers", "nimbus1:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt kafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("surbanski-twitter-hashtag-ranking"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()), "rankings"));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("strumien-twittera", new TwitterSpout());
        builder.setBolt("wydobycie-danych", new HashtagExtractorBolt(), 2).setNumTasks(4).shuffleGrouping("strumien-twittera");
        builder.setBolt("zliczanie-wystapien-hashtagow", new RollingCountBolt(20, 5)).fieldsGrouping
                ("wydobycie-danych", new Fields("hashtag"));
        builder.setBolt("ranking-posredni", new IntermediateRankingsBolt(10, 5, false)).fieldsGrouping
                ("zliczanie-wystapien-hashtagow", new Fields("hashtag"));
        builder.setBolt("ranking-glowny", new TotalRankingsBolt(10, 5, true)).globalGrouping("ranking-posredni");
        //builder.setBolt("nadawca-do-kafki", kafkaBolt).shuffleGrouping("ranking-glowny");

        return builder.createTopology();
    }
}
