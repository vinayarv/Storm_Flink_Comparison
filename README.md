# Storm_Flink_Comparison

The project aims to compare Storm and Flink, two popular open source distributed real-time computation systems. 


Presentation: http://bitly.com/sbInsight

#Introduction: 
Core storm and flink comparison without introducing any performance tunings in both of them.
It's not a full fledged comparison. 

#Pipeline:


#Insights:
1. Item 1Read happens first and then writes in storm.
2. Flink distrubutes the job as mini-pipeline(datasource-parse-count-sql). So, throughput and latency per window is very good.
3. Total tweets processed in storm is more compared to flink.
4. If I stall the kafka emission, like emit for a second and sleep for one sec, then storm gives good performace.


#Future Work:
1. Instead of counting, do some computation.
2. Introduce performance tuning for storm.


