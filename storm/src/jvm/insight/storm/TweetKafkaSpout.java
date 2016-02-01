//package insight.storm;
//
//import java.io.IOException;
//import java.util.Map;
//import java.util.StringTokenizer;
//import java.util.UUID;
//
//import org.codehaus.jackson.JsonNode;
//import org.codehaus.jackson.map.ObjectMapper;
//import org.json.JSONObject;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;
//
//import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import backtype.storm.StormSubmitter;
//import backtype.storm.generated.AlreadyAliveException;
//import backtype.storm.generated.AuthorizationException;
//import backtype.storm.generated.InvalidTopologyException;
//import backtype.storm.spout.SchemeAsMultiScheme;
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.topology.base.BaseBasicBolt;
//import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//import backtype.storm.utils.Utils;
//import storm.kafka.KafkaSpout;
//import storm.kafka.KeyValueSchemeAsMultiScheme;
//import storm.kafka.SpoutConfig;
//import storm.kafka.StringKeyValueScheme;
//import storm.kafka.StringScheme;
//import storm.kafka.ZkHosts;
//
//public class TweetKafkaSpout {
//	
//	public static class PrinterBolt extends BaseBasicBolt {
//
//		  @Override
//		  public void execute(Tuple tuple, BasicOutputCollector collector) {
//		    System.out.println(tuple);
//		  }
//
//		  @Override
//		  public void declareOutputFields(OutputFieldsDeclarer ofd) {
//		  }
//
//		}
//	public static class ParseTweetBolt extends BaseRichBolt
//	{
//	  // To output tuples from this bolt to the count bolt
//	  OutputCollector collector;
//	  private static final ObjectMapper mapper = new ObjectMapper();
//
//	  @Override
//	  public void prepare(
//	      Map                     map,
//	      TopologyContext         topologyContext,
//	      OutputCollector         outputCollector)
//	  {
//	    // save the output collector for emitting tuples
//	    collector = outputCollector;
//	  }
//
//	  @Override
//	  public void execute(Tuple tuple)
//	  {
//		  String tweet = tuple.getString(0);
//	        
//	            JsonNode root;
//				try {
//					root = mapper.readValue(tweet, JsonNode.class);
//				
//	            long id;
//	            String text;
//	            if (root.get("lang") != null &&
//	                "en".equals(root.get("lang").getTextValue()))
//	            {
//	                if (root.get("id") != null && root.get("text") != null)
//	                {
//	                    //id = root.get("id").getLongValue();
//	                    text = root.get("text").getTextValue();
//	                    collector.emit(new Values(text));
//	                }
//	            }
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}     
//		  
//	  }
//
//	  @Override
//	  public void declareOutputFields(OutputFieldsDeclarer declarer)
//	  {
//	    // tell storm the schema of the output tuple for this spout
//	    // tuple consists of a single column called 'tweet-word'
//	    declarer.declare(new Fields("tweet-word"));
//	  }
//	}
//	
//	public static void main(String[] args) throws Exception {
//		
//		TopologyBuilder builder = new TopologyBuilder();
//		ZkHosts hosts = new ZkHosts("52.20.10.138:2181,52.71.206.92:2181,52.22.151.39:2181");
//	    String kafkaTopic = "tweets";
//	    int kafkaPartitions = 1;
//	    String redisServerHost = "52.72.50.234";
//	    System.out.println( "\n" + kafkaTopic + "\n" + kafkaPartitions + "\n");
//	    
//	    SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
//	    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//	    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
//	    
//	    builder.setSpout("tweet-spout", kafkaSpout, kafkaPartitions);
//	    builder.setBolt("parse-bolt", new ParseTweetBolt(), 1).globalGrouping("tweet-spout");
//	    builder.setBolt("print-bolt", new PrinterBolt(),1).globalGrouping("parse-bolt");
//	    
//	    Config conf = new Config();
//
//        if (args != null && args.length > 0) {
//            conf.setNumWorkers(3);
//            //conf.setNumAckers(ackers);
//            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
//        }
//        else {
//
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("test", conf, builder.createTopology());
//            backtype.storm.utils.Utils.sleep(10000);
//            cluster.killTopology("test");
//            cluster.shutdown();
//        }
//	  
//	}
//
//}
