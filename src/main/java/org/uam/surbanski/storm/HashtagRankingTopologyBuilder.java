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
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt kafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("twitter-hashtag-ranking"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()), "rankings"));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-spout", new TwitterSpout());
        builder.setBolt("data-extractor", new HashtagExtractorBolt()).shuffleGrouping("twitter-spout");
        builder.setBolt("hashtag-counter", new RollingCountBolt(60, 5)).fieldsGrouping("data-extractor", new Fields("hashtag"));
        builder.setBolt("intermediate-ranker", new IntermediateRankingsBolt(10, 10, false)).fieldsGrouping
                ("hashtag-counter", new Fields("hashtag"));
        builder.setBolt("total-ranker", new TotalRankingsBolt(10, 10, true)).globalGrouping("intermediate-ranker");
        builder.setBolt("kafka-sender", kafkaBolt).shuffleGrouping("total-ranker");

        return builder.createTopology();
    }
}
