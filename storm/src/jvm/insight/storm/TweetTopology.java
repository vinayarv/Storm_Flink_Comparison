package insight.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


class TweetTopology
{
	
	public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */
//    Options opts = new Options();
//    opts.addOption("conf", true, "//home//ubuntu//flink_jar//benchmark.yaml");
//    //opts.addOption("conf", true, "//Users//vinayams//Documents//Insight//Storm_Flink_Comparison//TweetCountTopology//conf//benchmark.yaml");
//
//    CommandLineParser parser = new DefaultParser();
//    CommandLine cmd = parser.parse(opts, args);
//    String configPath = cmd.getOptionValue("conf");
//    //Map commonConfig = Utils.findAndReadConfigFile("//Users//vinayams//Documents//Insight//Storm_Flink_Comparison//TweetCountTopology//conf//benchmark.yaml", true);
//
//    Map commonConfig = Utils.findAndReadConfigFile(configPath, true);
//
//    String zkServerHosts = joinHosts((List<String>)commonConfig.get("zookeeper.servers"),
//                                     Integer.toString((Integer)commonConfig.get("zookeeper.port")));
//    String redisServerHost = (String)commonConfig.get("redis.host");
//    String kafkaTopic = (String)commonConfig.get("kafka.topic");
//    int kafkaPartitions = ((Number)commonConfig.get("kafka.partitions")).intValue();
//    int workers = ((Number)commonConfig.get("storm.workers")).intValue();
//    int ackers = ((Number)commonConfig.get("storm.ackers")).intValue();
//    int cores = ((Number)commonConfig.get("process.cores")).intValue();
//    int parallel = Math.max(1, cores/7);

    ZkHosts hosts = new ZkHosts("52.20.10.138:2181,52.71.206.92:2181,52.22.151.39:2181");
    String kafkaTopic = "tweets";
    int kafkaPartitions = 1;
    String redisServerHost = "52.72.50.234";
    int workers =3;
    int parallel = Math.max(1, 2*workers);
    System.out.println( "\n" + kafkaTopic + "\n" + kafkaPartitions + "\n");
    
    SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    
    
    builder.setSpout("tweet-spout", kafkaSpout, kafkaPartitions);
    builder.setBolt("parse-bolt", new ParseTweetBolt(), parallel).shuffleGrouping("tweet-spout");
    builder.setBolt("count-bolt", new CountBolt(), parallel).fieldsGrouping("parse-bolt", new Fields("tweet-word"));
    builder.setBolt("report-bolt", new ReportBolt(redisServerHost), parallel).globalGrouping("count-bolt");
    //builder.setBolt("print-bolt", new PrinterBolt(), 1).globalGrouping("count-bolt");


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

//    // create the default config object
//    Config conf = new Config();
//
//    // set the config in debugging mode
//    conf.setDebug(true);
//
//    if (args != null && args.length > 0) {
//
//      // run it in a live cluster
//
//      // set the number of workers for running all spout and bolt tasks
//      conf.setNumWorkers(3);
//
//      // create the topology and submit with config
//      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
//
//    } else {
//
//      // run it in a simulated local cluster
//
//      // set the number of threads to run - similar to setting number of workers in live cluster
//      conf.setMaxTaskParallelism(3);
//
//      // create the local cluster instance
//      LocalCluster cluster = new LocalCluster();
//
//      // submit the topology to the local cluster
//      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());
//
//      // let the topology run for 300 seconds. note topologies never terminate!
//      Utils.sleep(300000);
//
//      // now kill the topology
//      cluster.killTopology("tweet-word-count");
//
//      // we are done, so shutdown the local cluster
//      cluster.shutdown();
//    }
  }
}
