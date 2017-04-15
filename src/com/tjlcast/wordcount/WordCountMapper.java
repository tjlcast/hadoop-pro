package com.tjlcast.wordcount;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString() ;
		String[] split = line.split(" ") ;
		HashMap<String, Integer> dict = new HashMap<String, Integer>() ;
		
		for (String word : split) {
			dict.put(word, dict.get(word)!=null?dict.get(word)+1:1) ;
		}
		
		for (String word : dict.keySet()) {
			context.write(new Text(word), new IntWritable(dict.get(word)));
		}
	}


}
