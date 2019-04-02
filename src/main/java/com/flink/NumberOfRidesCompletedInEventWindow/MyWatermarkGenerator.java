package com.flink.taxi;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class MyWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>> {

	private static final long serialVersionUID = 5390275032968588707L;
	private final long maxOutOfOrderness = 2000;
	private long currentMaxTimestamp;
	long timestamp = 0;

	public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
		timestamp = element.f2;
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
		
	}

	public Watermark getCurrentWatermark() {

		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

}
