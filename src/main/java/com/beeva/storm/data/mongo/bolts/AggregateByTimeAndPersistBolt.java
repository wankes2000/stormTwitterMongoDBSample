package com.beeva.storm.data.mongo.bolts;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.MutableInt;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class AggregateByTimeAndPersistBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 780396805179790650L;
	private int aggregationTime;
	private MongoClient client;
	private MongoCollection<Document> collection;
	
	public AggregateByTimeAndPersistBolt(int aggregationTime) {
		this.aggregationTime = aggregationTime;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		try {
			client = new MongoClient(new MongoClientURI("mongodb://172.17.0.2:27017"));
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    MongoDatabase db = client.getDatabase("test");
	    collection = db.getCollection("report_data_" + aggregationTime);
	}

	
	
	
	
	@Override
	public void execute(Tuple tuple) {
	
	  final String word = tuple.getStringByField("word");
		Integer count =  tuple.getIntegerByField("count");
	  Document document = new Document();
		document.put("date", new Date());
		document.put("word", word);
		document.put("count", count);
		collection.insertOne(document);
	}

	
	

	@Override
	public void cleanup() {

		client.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
}