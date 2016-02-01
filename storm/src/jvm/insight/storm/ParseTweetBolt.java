package insight.storm;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.codehaus.jackson.JsonNode;
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
		try {
			root = mapper.readValue(tweet, JsonNode.class);
		
      long id;
      String text;
      if (root.get("lang") != null &&
          "en".equals(root.get("lang").getTextValue()))
      {
          if (root.get("id") != null && root.get("text") != null)
          {
              //id = root.get("id").getLongValue();
              text = root.get("text").getTextValue();
              StringTokenizer tokenizer = new StringTokenizer(text);
              // split the message
				while (tokenizer.hasMoreTokens()) {
					String token = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
					collector.emit(new Values(token));
				}
              
          }
      }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}     
	  
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare(new Fields("tweet-word"));
  }
}
