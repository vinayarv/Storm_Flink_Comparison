package insight.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import org.json.*;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TweetProducer {
	public static void main(String[] args) {
        
		if (!parseParameters(args)) {
			return;
		}
 
        Properties props = new Properties();
		//Provide kafka zookeeper ip address
        props.put("metadata.broker.list", "X.X.X.X:9092,5X.X.X.X:9092,X.X.X.X:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);

        if(isFilePath){
        	for (int i=0; i<500; i++){

	        	File fin = new File(inputFile);
	        	FileInputStream fis;
				try {
					fis = new FileInputStream(fin);
					//Construct BufferedReader from InputStreamReader
		        	BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		         
		        	String line = null;
		        	while ((line = br.readLine()) != null) {
		        		JSONObject jObj = new JSONObject(line);
		        		if(jObj.has("timestamp_ms")){
		        			//System.out.println(jObj.get("timestamp_ms"));
		        			jObj.put("timestamp_ms", System.currentTimeMillis());
		        			KeyedMessage<String, String> tweet = new KeyedMessage<String, String>(topicname, jObj.toString());
			        		producer.send(tweet);
		        			//System.out.println(jObj.get("timestamp_ms"));
		        		}
		        	}
		         
		        	br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		
        	}
        	
        }
        producer.close();
    }
	
	public static String inputFile, topicname;
	public static boolean isFilePath = false;
	private static boolean parseParameters(String[] args) {
		if (args.length > 0) {
			isFilePath = true;
			inputFile = args[0];
			topicname = args[1];
		}
		return isFilePath;
	}
		
}
