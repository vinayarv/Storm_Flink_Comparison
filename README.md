

# Introduction: 
The project aims to compare Storm and Flink, two popular open source distributed real-time computation systems. The objective is to compare the technologies by considering a straighforward implementation each one provides.

#Pipeline:
<img src="/images/pipeline.png" width="600">

Twitter data is considered for processing. When ingesting the twitter data from kafka, I assign a read_ts to each record.
read_ts provides actual emitted time in milli-second from kafka. In storm/flink, each incoming record is parsed based on language. Since I don't want to lose any record as I'm calculating throughput(number of records processed/sec), parse class just checks whether the record language is "english" or not. Each record's tweet data along with timestamp is passed to compute class to count the number of characters in the tweet. The result of the compute class is then sent to a sql class which writes the (read_ts,write_ts) to the MYSQL DB. write_ts indicates the end of processing cycle.

The program does not consider any performance tunings for both the technologies while comparing.

#Insights:
* High throughput and low latency is observed with Apache Flink when considering a time window as shown in Flink graphs in results section.
* With storm, I observed that storm tries to read the Kafka emitted data first, and then does the processing[shown in 1st graph: Storm Read/Write Distribution]. So calculating the throughput/latency per window is not the right measure.
If I had considered window/tick_tuple method of implementation, I could have claculated throughput/latency per window.
* The total tweets processed in storm is high compared to that in Flink.

Flink distrubutes the job as mini-pipeline(datasource-parse-count-sql) as shown below. So, throughput and latency per window is very good.
![GitHub Logo](/images/flink_job_scheduling.png)

In order to get better performance per window, I tried below mentioned tunings in Storm.
* Decreased the number of kafka consumers and increased the number of compute bolts.
* Tried different groupings.

One of the observations is, when storm makes connection with kafka, it tries to read all the emitted records first. If I manage kafka emission per window; say in a 10 second window if I emit for first 5 seconds and not for another 5 seconds then we may perhaps see good read/write distribution.

#Results:
The results along with the calculation method is discussed in the presentation: http://bitly.com/sbInsight


<img src="/images/storm_distrubution.png" width="400"><img src="/images/flink_distribution.png" width ="400">
<img src="/images/flink_throughput.png" width="450"><img src="/images/flink_latency.png" width="400">

#Conclusion:
* If a user wants fast real-time computation per window then Flink is better choice.
* If a user considers overall throughput, then storm is the choice.

Both of the above suggestions are made considering a straigh-forward implementation without window/tick_tuple or performance tunings.

#Future Work:
1. Program for the architecture in Storm. (Manage Kafka emissions as discussed in the Insight section)
2. Consider more complex computation instead of counting.
3. Introduce performance tuning in both the technolgies.
4. Consider using window/tick_tuple implementation(though people suggested its not the right way to compare)



