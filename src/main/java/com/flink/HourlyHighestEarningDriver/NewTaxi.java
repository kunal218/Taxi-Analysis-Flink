package com.flink.taxi;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
		 env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "consumer-new");
		// properties.setProperty("auto.offset.reset","earliest");
		properties.setProperty("auto.offset.reset", "latest");

		// properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,System.currentTimeMillis()+"");
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer<String>("advtaxi", new SimpleStringSchema(), properties));

		stream.map(new MyMapper())

				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<TaxiPojo, Integer>>() {
					private final long maxOutOfOrderness = 1000;
					private long currentMaxTimestamp;
					long timestamp = 0;

					private static final long serialVersionUID = 3284927561288988855L;

					public long extractTimestamp(Tuple2<TaxiPojo, Integer> element, long previousElementTimestamp) {
						timestamp = element.f0.getPickUpTimestamp();
						currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
						return timestamp;
					}

					public Watermark getCurrentWatermark() { // TODO Auto-generated method stub
						return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
					}
				})

				.timeWindowAll(Time.seconds(10))
				
				.apply(new MyWindowFunction())

				.print();

		

		env.execute();
	}

}
