package com.flink.taxi;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyWindowFunction
		implements WindowFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>, Tuple, TimeWindow> {

	int windowCounter = 0;

	private static final long serialVersionUID = 7914655115431649968L;

	public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input,
			Collector<Tuple2<String, Integer>> out) throws Exception {
		String trip = null;
		int count = 0;
		System.out.println("Window " + (++windowCounter) + " ended");
		for (Tuple3<String, Integer, Long> tuple : input) {
			System.out.println("Elements for window " + tuple.toString());
			trip = tuple.f0;
			count++;
		}

		out.collect(new Tuple2<String, Integer>(trip, count));
	}
}