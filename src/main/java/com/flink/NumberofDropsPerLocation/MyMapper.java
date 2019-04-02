package com.flink.taxi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.myflink.taxipojo.TaxiPojo;

public class MyMapper implements MapFunction<String, Tuple2<TaxiPojo, Integer>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2730387930453613688L;

	public Tuple2 map(String value) throws Exception {
		String[] data = value.split(",");
		return new Tuple2<TaxiPojo, Integer>(new TaxiPojo(data[0],data[1] , Long.parseLong(data[2]), data[3], Long.parseLong(data[4]),Long.parseLong( data[5])),1);
	}
}
