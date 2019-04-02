package com.flink.taxi;

import java.awt.List;
import java.util.*;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.myflink.taxipojo.TaxiPojo;

public class MyWindowFunction
		extends RichAllWindowFunction<Tuple2<TaxiPojo,Integer>, Tuple2<String, Integer>, TimeWindow>{

	int windowCounter = 0;
	
	HashMap<String, Integer> map;
	private static final long serialVersionUID = 7914655115431649968L;
	@Override
	public void open(Configuration parameters) throws Exception {
		 map = new HashMap<String, Integer>();
	}

	
	public java.util.List<Map.Entry<String, Integer>> sortbyvalue(HashMap<String, Integer> map) {
		LinkedList<Map.Entry<String, Integer>> list= 
	               new LinkedList<Map.Entry<String, Integer> >(map.entrySet()); 
	  
	        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() { 
	            public int compare(Map.Entry<String, Integer> o1,  
	                               Map.Entry<String, Integer> o2) 
	            { 
	                return (o1.getValue()).compareTo(o2.getValue()); 
	            } 
	        });
	       
	       
	        return list;
	}
	public void apply(TimeWindow window, Iterable<Tuple2<TaxiPojo, Integer>> values,
			Collector<Tuple2<String, Integer>> out) throws Exception {
		// TODO Auto-generated method stub
		int count = 0;
		  //map.clear();
	       
		for(Tuple2<TaxiPojo, Integer> tuple:values) {
			System.out.println("\n Tuple in window :"+tuple.f0.toString()+" :"+tuple.f1);
			if(!(map.containsKey(tuple.f0.getDriverName()))) {
				map.put(tuple.f0.getDriverName(), tuple.f1);
				System.out.println("\n Putting in MAP -> "+tuple.f0.getDriverName()+" :"+tuple.f1);
			}
			else {
				Integer fare=map.get(tuple.f0.getDriverName());
					map.put(tuple.f0.getDriverName(), (tuple.f1+fare));
			}
			
			  
		        
		       
		}
		
		java.util.List<Entry<String, Integer>> sortedList = sortbyvalue(map);
		map.clear();
	       Entry<String, Integer> lastEntry = sortedList.get(sortedList.size()-1);
	       
	       out.collect(new Tuple2<String, Integer>(lastEntry.getKey(), lastEntry.getValue()));
	     
		System.out.println("Outside Collect...clearing");
		
		
	}

	
	
	
	
}
