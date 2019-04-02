package com.flink.taxi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyMapper implements MapFunction<String, Tuple3< String, Integer,Long>>{

	public Tuple3< String, Integer,Long> map(String value) throws Exception {
		String[] data = value.split(",");
		return (new Tuple3< String, Integer,Long>(data[1].toString()+data[2].toString(),1,new Long(data[0])));
	}
}
