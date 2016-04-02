/*
 * Copyright 2012-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.beeva.storm.data.mongo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.beeva.storm.data.mongo.bolts.AggregateByTimeAndPersistBolt;
import com.beeva.storm.data.mongo.bolts.TweetSplitterBolt;
import com.beeva.storm.data.mongo.bolts.WordCounterBolt;
import com.beeva.storm.data.mongo.spots.TweetsStreamingConsumerSpout;

@Configuration
@EnableAutoConfiguration
@ComponentScan
@EnableMongoRepositories
public class SampleMongoApplication implements CommandLineRunner {



	@Override
	public void run(String... args) throws Exception {
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterSpout", new TweetsStreamingConsumerSpout());
		builder.setBolt("tweetSplitterBolt", new TweetSplitterBolt(), 10).shuffleGrouping("twitterSpout");
		builder.setBolt("wordCounterBolt", new WordCounterBolt(10), 10).fieldsGrouping("tweetSplitterBolt", new Fields("word"));
		builder.setBolt("aggregateByMinuteAndPersistBolt", new AggregateByTimeAndPersistBolt(10), 10).fieldsGrouping("wordCounterBolt", new Fields("word"));

		final Config conf = new Config();
		conf.setDebug(false);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("wordCountTopology", conf, builder.createTopology());
		
		
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(SampleMongoApplication.class, args);
	}
}
