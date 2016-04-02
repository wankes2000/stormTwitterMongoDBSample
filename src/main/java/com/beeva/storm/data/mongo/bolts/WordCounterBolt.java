package com.beeva.storm.data.mongo.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCounterBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3364382152456417216L;
	private OutputCollector collector = null;
	private Map<String, Integer> words = null;
	private Integer aggregationTime;

	
	
	/**
	 * @param aggregationTime
	 */
	public WordCounterBolt(Integer aggregationTime) {
		super();
		this.aggregationTime = aggregationTime;
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.words = new HashMap<String, Integer>();
	}

	@Override
	public void execute(Tuple input) {
		if (isTickTuple(input)){
			if (words!=null){
				for (String word : words.keySet()){
					collector.emit(new Values(word, words.get(word)));
				}
			}
			words = new HashMap<String, Integer>();
		}else {
			final String word = input.getStringByField("word");
			Integer count = words.get(word);
			if (count == null) {
				count = 0;
			}
			count++;

			words.put(word, count);
		}
		
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	    Config conf = new Config();
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, aggregationTime);
	    return conf;
	}

	private static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
	
}