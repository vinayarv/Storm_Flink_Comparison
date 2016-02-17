

# Introduction: 
The project aims to compare Storm and Flink, two popular open source distributed real-time computation systems. The objective is to compare the technologies by considering a straighforward implementation each one provides.

#Pipeline:
![GitHub Logo](/images/pipeline.png)

Twitter data is considered for processing. When ingesting the twitter data from kafka, I assign a read_ts to each record.
read_ts provides actual emitted time in milli-second from kafka. In storm/flink, after consuming the data I parse the tweets based on language. Since I don't want to lose the record as I'm calculating throughput, I just checked whether the language is "english" is parse program. After parsing, I pass the records tweet data along with timestamp to compute class to count the number of characters in the tweet. The result of the compute class is then sent to a class that make DB connection. This class writes the (read_ts,write_ts) to the MYSQL DB. write_ts indicates the end of processing cycle.

The program does not consider any performance tunings for both the technologies while comparing.

#Insights:
* High throughput and low latency is observed with Apache Flink when considering a time window.
* With storm, I observed that storm tries to read the Kafka emitted data first, and then does the processing. So calculating the throughput/latency per window is not the right measure.
If I had considered window/tick_tuple method of implementation, I could have claculated throughput/latency per window.
* The total tweets processed in storm is high compared to that in Flink.

![GitHub Logo](/images/flink_job_scheduling.png)

Flink distrubutes the job as mini-pipeline(datasource-parse-count-sql). So, throughput and latency per window is very good.

Below is the list of the tunings I considered for storm.
* Decreased number of kafka consumers and increased number of compute bolts.
* Tried different groupings.

When storm makes connection with kafka, it tries to read all the emitted records first. If I manage kafka emission per window; say emit tweets for every 10 seconds and stop for the next 10 seconds, storm performs better.

#Results:
The results along with the calculation method is discussed in the presentation: http://bitly.com/sbInsight
<img src="/images/storm_distrubution.png" width="300"><img src="/images/flink_distribution.png" width ="300">


#Conclusion:
* If a user wants fast real-time computation per window then Flink is better choice.
* If a user considers overall throughput, then storm is the choice.
Both of the above suggestions are made considering a straigh-forward implementation without window/tick_tuple or performance tunings.

#Future Work:
1. Consider more complex computation instead of counting.
2. Introduce performance tuning in both the technolgies.
3. Consider using window/tick_tuple implementation(though people suggested its not the right way to compare)



