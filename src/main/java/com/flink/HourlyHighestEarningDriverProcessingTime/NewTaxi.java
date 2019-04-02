package com.flink.taxi;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.myflink.taxipojo.TaxiPojo;

import org.apache.flink.streaming.api.functions.*;

public class NewTaxi {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		//env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "consumer-new");
		// properties.setProperty("auto.offset.reset","earliest");
		properties.setProperty("auto.offset.reset", "latest");

		// properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,System.currentTimeMillis()+"");
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer<String>("advtaxi", new SimpleStringSchema(), properties));

		stream.map(new MyMapper())
		.keyBy(new KeySelector<Tuple2<TaxiPojo,Integer>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String getKey(Tuple2<TaxiPojo, Integer> value) throws Exception {
				// TODO Auto-generated method stub
				String driverName = value.f0.getDriverName();
				return driverName;
			}
		})
	   
	   .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
	   
	   
	 //   .allowedLateness(Time.seconds(20))
	    .apply(new WindowFunction<Tuple2<TaxiPojo,Integer>, Tuple2<String,Integer>, String, TimeWindow>() {
	    	/**
			 * 
			 */
			private static final long serialVersionUID = 4797014506493231231L;
			int windowCounter = 0;
			public void apply( String key, TimeWindow window, Iterable<Tuple2<TaxiPojo, Integer>> input,
					Collector<Tuple2<String, Integer>> out) throws Exception {
				int sum = 0;
				TaxiPojo taxi =null;
				String driverName=null;
				System.out.println("Window " + (++windowCounter) + " ended");
				for(Tuple2<TaxiPojo, Integer> tuple:input) {
					taxi=tuple.f0;
					sum+=tuple.f0.getFare();
					driverName=tuple.f0.getDriverName();
				}
				out.collect(new Tuple2<String, Integer>(driverName,sum));
				
			}
		})
	    .timeWindowAll(Time.seconds(5))
	    .maxBy(1)
	    .print();
		

		env.execute();
	}

}
