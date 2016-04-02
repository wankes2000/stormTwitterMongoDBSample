package com.beeva.storm.data.mongo.spots;

import static com.beeva.storm.data.mongo.constants.GenericConstants.TWITTER4J_ACCESS_TOKEN_ENV;
import static com.beeva.storm.data.mongo.constants.GenericConstants.TWITTER4J_ACCESS_TOKEN_SECRET_ENV;
import static com.beeva.storm.data.mongo.constants.GenericConstants.TWITTER4J_DEBUG_ENV;
import static com.beeva.storm.data.mongo.constants.GenericConstants.TWITTER4J_OAUTH_CONSUMER_KEY_ENV;
import static com.beeva.storm.data.mongo.constants.GenericConstants.TWITTER4J_OAUTH_CONSUMER_SECRET_ENV;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TweetsStreamingConsumerSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4653254474692735785L;
	private SpoutOutputCollector collector;
	private LinkedBlockingQueue queue;
	private TwitterStream twitterStream;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setDebugEnabled(Boolean.getBoolean(System.getenv(TWITTER4J_DEBUG_ENV)))
	      .setOAuthConsumerKey(System.getenv(TWITTER4J_OAUTH_CONSUMER_KEY_ENV))
	      .setOAuthConsumerSecret(System.getenv(TWITTER4J_OAUTH_CONSUMER_SECRET_ENV))
	      .setOAuthAccessToken(System.getenv(TWITTER4J_ACCESS_TOKEN_ENV))
	      .setOAuthAccessTokenSecret(System.getenv(TWITTER4J_ACCESS_TOKEN_SECRET_ENV));
		this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		this.queue = new LinkedBlockingQueue();
		
		final StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception e) {
			}

			@Override
			public void onStallWarning(StallWarning warning) {
			}
		};

		twitterStream.addListener(listener);
	}

	@Override
	public void nextTuple() {
		final Status status = (Status) queue.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(status));
		}
	}

	@Override
	public void activate() {
		twitterStream.sample();
	};
	
	@Override
	public void deactivate() {
		twitterStream.cleanUp();
	};

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status"));
	}
}