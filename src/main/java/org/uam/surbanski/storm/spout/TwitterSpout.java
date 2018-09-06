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

import java.util.*;
import java.util.concurrent.TimeUnit;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private TwitterStream twitterStream;
    private Map queue;
    private Map controlQueue;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();


        // tworze obiekt strumienia
        this.twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

        // kolejka na ktorej bede przechowywal w pamieci wiadomosci wyslane z Twittera, a spout bedzie z niej czytal
        this.queue = Collections.synchronizedMap(new LinkedHashMap<UUID, Status>());
        this.controlQueue = Collections.synchronizedMap(new LinkedHashMap<UUID, Status>());

        // API Twittera wymaga uzycia obiektu klasy StatusListener, ktory kontroluje wysylane wiadomosci
        twitterStream.addListener(new StatusListener() {
            public void onException(Exception e) {

            }

            public void onStatus(Status status) {
                //System.out.println(status);
                UUID id = UUID.randomUUID();

                queue.put(id, status);
                controlQueue.put(id, status);
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

        // zapytanie, ktore pozwoli odfiltrowac interesujace nas tweety z Europy Zachodniej ze strumienia wszystkich
        // dostepnych tweetow
        final FilterQuery filterQuery = new FilterQuery();
        filterQuery.locations(new double[]{-9.17, 38.45}, new double[]{21.47, 54.3});
        twitterStream.filter(filterQuery);

    }


    @Override
    public void nextTuple() {

        Status entry;
        Set s = queue.keySet();
        // This block is equivalent to polling

        synchronized (queue) {
            Iterator i = s.iterator(); // Must be in the synchronized block
            if (!i.hasNext()) {
                Utils.sleep(50);
            } else {
                UUID currId = (UUID) i.next();
                entry = (Status) queue.get(currId);
                spoutOutputCollector.emit(new Values(entry), currId);
                i.remove();
            }
        }


        // Status status = queue.poll();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public void ack(Object msgId) {
        UUID messageId = (UUID) msgId;
        System.out.println("suckes!!!!!!!!!!!!!!!!!!!!!!!");
        controlQueue.remove(messageId);
    }

    @Override
    public void fail(Object msgId) {
        UUID messageId = (UUID) msgId;
        System.out.println("Tweet z id " + messageId + " nie zostal calkowicie przetworzony");
        //System.out.println(queue.get(messageId).toString());
        queue.put(messageId, controlQueue.get(messageId));
        System.out.println(queue.get(messageId).toString());
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }
}
