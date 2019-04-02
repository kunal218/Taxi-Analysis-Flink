package com.flink.taxi;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class AggregationProcessingTimeWindows {

	public static void main(String[] args) throws Exception {
		
		 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		  
		    Properties properties = new Properties();
		    properties.setProperty("bootstrap.servers", "localhost:9092");
	//    properties.setProperty("group.id", "consumer-new");
		    properties.setProperty("auto.offset.reset","earliest");
		    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,System.currentTimeMillis()+"");
		    DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<String>("taxi1", new SimpleStringSchema(), properties));
		 
		    stream.map(new MapFunction<String, Tuple4<Long, String, String,Integer>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

			

				public Tuple4<Long, String, String,Integer> map(String value) throws Exception {
					String[] data = value.split(",");
					return (new Tuple4<Long, String, String,Integer>(new Long(data[0]),data[1].toString(),data[2].toString(),1));
				}
			})
		    .keyBy(3)
		    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
		    .sum(3)
		 
		 .print();
		    env.setParallelism(1);
		    env.execute();
	}

}
