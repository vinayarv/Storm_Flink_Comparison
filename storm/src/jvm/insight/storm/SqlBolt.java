package insight.storm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SqlBolt extends BaseRichBolt{
	
	// JDBC driver name and database URL
	static final String JDBC_DRIVER ="com.mysql.jdbc.Driver";   
	static final String DB_URL= "jdbc:mysql://X.X.X.X:3306/SF_BENCHMARK";
	   //  Database credentials
	static final String USER= "admin";
	static final String PASS = "";
	
	private Connection connect = null;
	private PreparedStatement preparedStatement = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(DB_URL,USER,PASS);
			
			connect.createStatement();
		    
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
	    try {
	    	
	    	long rk_ts = input.getLongByField("rk_ts");
	    	Timestamp read_kts = new Timestamp(rk_ts);
	    	
	    	long rp_ts = input.getLongByField("rp_ts");
	    	Timestamp read_pts = new Timestamp(rp_ts);
	    	
	    	long wc_ts = input.getLongByField("wc_ts");
	    	Timestamp read_cts = new Timestamp(wc_ts);
	    	
	    	long w_ts = System.currentTimeMillis();
	    	Timestamp write_ts = new Timestamp(w_ts);
	    	    	
	    	String query = "insert into STORM_TIME(KAFKA_TS, CONS_TS, COUNT_TS, WRITE_TS) values(?, ?, ?, ?)";
			preparedStatement = connect.prepareStatement(query);
			preparedStatement.setTimestamp(1, read_kts);
			preparedStatement.setTimestamp(2, read_pts);
			preparedStatement.setTimestamp(3, read_cts);
			preparedStatement.setTimestamp(4, write_ts);
			preparedStatement.executeUpdate();
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		} 
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
