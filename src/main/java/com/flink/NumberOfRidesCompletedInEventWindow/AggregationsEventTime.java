package com.flink.taxi;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.streaming.api.functions.*;

public class AggregationsEventTime {

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
				.addSource(new FlinkKafkaConsumer<String>("taxi18", new SimpleStringSchema(), properties));

		stream.map(new MyMapper())
		.assignTimestampsAndWatermarks(new MyWatermarkGenerator())
		.keyBy(0)
		.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
		// .allowedLateness(Time.seconds(1))
		// .sum(1)

		.apply(new MyWindowFunction())
		.setParallelism(1)
		.print();  
		// .writeAsText("testWindow",WriteMode.OVERWRITE);

		env.execute();
	}

}
