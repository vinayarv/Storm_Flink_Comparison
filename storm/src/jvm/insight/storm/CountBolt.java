package insight.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


import java.util.HashMap;
import java.util.Map;

/**
 * A bolt that counts the characters in each tweet
 */
public class CountBolt extends BaseRichBolt
{

/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

// To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

  }

  @Override
  public void execute(Tuple tuple)
  {
    String tweet = tuple.getStringByField("tweet_text");
    long rp_ts = tuple.getLongByField("r_ts");
    long rk_ts = tuple.getLongByField("rk_ts");   
    int count = tweet.length();
    long wc_ts = System.currentTimeMillis();
    collector.emit(new Values(rk_ts, rp_ts, wc_ts));
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {

    outputFieldsDeclarer.declare(new Fields("rk_ts", "rp_ts", "wc_ts"));
  }
}
