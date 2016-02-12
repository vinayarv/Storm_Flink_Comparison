package insight.flink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONException;


public class TweetCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>(parameterTool.getRequired("topic"), 
				                           new SimpleStringSchema(), parameterTool.getProperties()));
	
		messageStream.flatMap(new ParseTweetBolt()).
		flatMap(new CountBolt()).
		flatMap(new SqlBolt());
		

		// execute program
		env.execute("Tweet Count Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	
	public static class ParseTweetBolt extends JSONParseFlatMap<String, Tuple4<Long, Long, String, Boolean>> {
		

		/**
		 * Select the language from the incoming JSON text
		 */
		@Override
		public void flatMap(String value, Collector<Tuple4<Long, Long, String, Boolean>> out) throws Exception {
			try {
				String tweet_text= getString(value, "text");
				long rk_ts = getLong(value, "timestamp_ms");
				long rp_ts = System.currentTimeMillis();
				boolean isEnglish = false;
				if (getString(value, "user.lang").equals("en")) {
					//message of tweet
					isEnglish =true;
					tweet_text = tweet_text.replaceAll("\\s*", "").toLowerCase();
				}
				out.collect(new Tuple4<>(rk_ts, rp_ts, tweet_text, isEnglish));	
				
			} catch (JSONException e) {
				System.out.println("JSON not parsed correctly");
				// the JSON was not parsed correctly
			}
			
		}
	}
	
	public static class CountBolt implements
    FlatMapFunction<Tuple4<Long, Long, String, Boolean>, Tuple3<Long, Long, Long>> {

		@Override
		public void flatMap(Tuple4<Long, Long, String, Boolean> value, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
			
			Long rk_ts = value.getField(0);
			Long rp_ts = value.getField(1);
			String tweet = value.getField(2);
			
			if(value.getField(3)){
				int count = tweet.length();
			}else{
				int count =0;
			}
			Long wc_ts = System.currentTimeMillis();
			
			out.collect(new Tuple3<>(rk_ts, rp_ts, wc_ts));
		}
	}
	
    public static class SqlBolt implements FlatMapFunction<Tuple3<Long, Long, Long>, String> {
    	
    	private Connection connect = null;
    	private PreparedStatement preparedStatement = null;
    	
    	@Override
		public void flatMap(Tuple3<Long, Long, Long> timestamp, Collector<String> out) throws Exception {
			
			connect = getDatabaseConnection();
			if (connect != null){
				
				Long rk_ts = timestamp.getField(0);
				Timestamp read_kts = new Timestamp(rk_ts);
				
				Long rp_ts = timestamp.getField(1);
				Timestamp read_pts = new Timestamp(rp_ts);
				
				Long wc_ts = timestamp.getField(2);
				Timestamp read_cts = new Timestamp(wc_ts);
				
		    	long w_ts = System.currentTimeMillis();
		    	Timestamp write_ts = new Timestamp(w_ts);
		    	
		    	
		    	String query = "insert into T_FLINK_EXTR_COL(READ_KAFKA_TS, READ_TS_PROD, WC_TS, WRSQL_TS) values(?, ?, ?, ?)";
				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setTimestamp(1, read_kts);
				preparedStatement.setTimestamp(2, read_pts);
				preparedStatement.setTimestamp(3, read_cts);
				preparedStatement.setTimestamp(4, write_ts);
				preparedStatement.executeUpdate();
			}		
			
		}	
		
    }

	public static Connection getDatabaseConnection() {
		final String JDBC_DRIVER ="com.mysql.jdbc.Driver";   
    	final String DB_URL= "jdbc:mysql://X.X.X.X:3306/dbname?useSSL=false";
    	   //  Database credentials
    	final String USER= "admin";
    	final String PASS = "";
    	Connection connect = null;
    	try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(DB_URL,USER,PASS);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			return connect;			
		}
		return connect;
		   	
		
	}
}
