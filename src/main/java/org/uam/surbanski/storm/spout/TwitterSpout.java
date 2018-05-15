package org.uam.surbanski.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private TwitterStream twitterStream;
    private LinkedBlockingQueue<Status> queue;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey("<wstaw dane swojego konta na Twitterze>")
                .setOAuthConsumerSecret("<wstaw dane swojego konta na Twitterze>")
                .setOAuthAccessToken("<wstaw dane swojego konta na Twitterze>")
                .setOAuthAccessTokenSecret("<wstaw dane swojego konta na Twitterze>");

        // tworze obiekt strumienia
        this.twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

        // kolejka na ktorej bede przechowywal w pamieci wiadomosci wyslane z Twittera, a spout bedzie z niej czytal
        this.queue = new LinkedBlockingQueue<Status>();

        // API Twittera wymaga uzycia obiektu klasy StatusListener, ktory kontroluje wysylane wiadomosci
        twitterStream.addListener(new StatusListener() {
            public void onException(Exception e) {

            }

            public void onStatus(Status status) {
                System.out.println("DEBUG SU - " + status.getText() + " - " + status.getUser().getLocation());
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }
        });

        // zapytanie, ktore pozwoli odfiltrowac interesujace nas tweety ze wszystkich dostepnych
        final FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(new String[]{"chocolate"});
        twitterStream.filter(filterQuery);

    }

    @Override
    public void nextTuple() {
        Status status = queue.poll();
        if (status == null) {
            Utils.sleep(50);
        } else {
            spoutOutputCollector.emit(new Values(status));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }
}
