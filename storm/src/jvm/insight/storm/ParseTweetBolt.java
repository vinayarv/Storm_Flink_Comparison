package insight.storm;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



/**
 * A bolt that parses the tweet into words
 */
public class ParseTweetBolt extends BaseRichBolt
{
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
// To output tuples from this bolt to the count bolt
  OutputCollector collector;
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple)
  {
	  String tweet = tuple.getString(0);
	  JsonNode root;
	  long r_ts;
	  String tweet_text;
      
	  try {
		  
		  root = mapper.readValue(tweet, JsonNode.class);
		  long rk_ts = root.get("timestamp_ms").getLongValue();
		  r_ts = System.currentTimeMillis();
		  tweet_text = root.get("text").getTextValue();
	        if (root.get("lang") != null &&
	          "en".equals(root.get("lang").getTextValue()))
	        {    	
	            tweet_text = tweet_text.replaceAll("\\s*", "").toLowerCase();
	              
	        }
	        
	        collector.emit(new Values(rk_ts,r_ts,tweet_text));  
	         
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}     
	  
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("rk_ts","r_ts", "tweet_text"));
  }
}
