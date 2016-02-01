package insight.storm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

/**
 * A bolt that prints the word and count to redis
 */
@SuppressWarnings("serial")
public class ReportBolt extends BaseRichBolt implements Serializable
{

	private Jedis jedis;
	private String redisServerHost;
	private int port;
	
	public ReportBolt(String redisServerHost) {
		super();
		this.redisServerHost = redisServerHost;
		this.port = 6379;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.jedis = new Jedis(redisServerHost,port);
		System.out.println("Connected to Redis");
		
	}
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		// access the first column 'word'
	      String word = input.getStringByField("word");

	      // access the second column 'count'
	      int count = input.getIntegerByField("count");
	      
	      jedis.lpush(Integer.toString(count),word);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}