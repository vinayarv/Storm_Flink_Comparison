package insight.storm;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


public class TweetTopology
{
	
	public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();


    ZkHosts hosts = new ZkHosts("X.X.X.X:2181,X.X.X.X:2181,X.X.X.X:2181");
    String kafkaTopic = "storm18";
    int kafkaPartitions = 3;
    int workers =3;
    int parallel = Math.max(1, 2*workers);
    System.out.println( "\n" + kafkaTopic + "\n" + kafkaPartitions + "\n");
    
    SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    
    
    builder.setSpout("tweet-spout", kafkaSpout, kafkaPartitions);
    builder.setBolt("parse-bolt", new ParseTweetBolt(), parallel*3).shuffleGrouping("tweet-spout");
    builder.setBolt("count-bolt", new CountBolt(), parallel*3).shuffleGrouping("parse-bolt");
    builder.setBolt("report-bolt", new SqlBolt(), parallel*2).shuffleGrouping("count-bolt");

    //*********************************************************************
    
    Config conf = new Config();

    if (args != null && args.length > 0) {
        conf.setNumWorkers(workers);
        //conf.setNumAckers(ackers);
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
       
        
    }
    else {

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        backtype.storm.utils.Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

 
  }
	
}
