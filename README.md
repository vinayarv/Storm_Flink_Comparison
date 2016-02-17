

Presentation: http://bitly.com/sbInsight

# Introduction: 
The project aims to compare Storm and Flink, two popular open source distributed real-time computation systems. 

#Pipeline:
![GitHub Logo](/images/pipeline.png)

Twitter data is considered for processing. When ingesting the twitter data from kafka, I assign a read_ts to each record.
read_ts provides actual emitted time in milli-second from kafka. In storm/flink, after consuming the data I parse the tweets based on language. If language is "english" I send the the data to next compute class(Count bolt). This compute class counts characters in each tweet and send the read_ts to Database class. Database class makes db connection and writes (read_ts,write_ts) to database marking end of computation. write_ts indicates the end of processing cycle.

The program does not consider any performance tunings for both the technologies while comparing.

#Insights:
1. Read happens first and then writes in storm.
2. Flink distrubutes the job as mini-pipeline(datasource-parse-count-sql). So, throughput and latency per window is very good.
3. Total tweets processed in storm is more compared to flink.
4. If I stall the kafka emission, like emit for a second and sleep for one sec, then storm gives good performace.


#Future Work:
1. Instead of counting, do some computation.
2. Introduce performance tuning for storm.


