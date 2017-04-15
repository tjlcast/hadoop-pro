package com.tjlcast.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration() ;
		System.setProperty("HADOOP_USER_NAME", "root"); // set jvm user_name
		conf.set("fs.defaultFS", "hdfs://10.108.217.142:9000"); // remote need
		conf.set("mapreduce.framework.name", "yarn") ; // remote need
		conf.set("yarn.resourcemanager.hostname", "10.108.217.142"); // remote need
		
		Job job = Job.getInstance(conf) ;
		job.setJar("/Users/tangjialiang/Desktop/WordCount.jar"); // remote need
//		job.setJarByClass(FlowDriver.class); // local need
		
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(FlowReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// remote need
		FileSystem fs = FileSystem.get(conf);
		boolean exists = fs.exists(new Path("/tangjialiang/mr/flow/output/day")) ;
		if(exists) fs.delete(new Path("/tangjialiang/mr/flow/output/day"), true) ;
		FileInputFormat.setInputPaths(job, new Path("/tangjialiang/mr/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("/tangjialiang/mr/flow/output/day"));
		
		// local need
//		FileSystem fs = FileSystem.get(conf);
//		boolean exists = fs.exists(new Path("/Users/tangjialiang/Desktop/flow/output")) ;
//		if(exists) fs.delete(new Path("/Users/tangjialiang/Desktop/flow/output"), true) ;
//		FileInputFormat.setInputPaths(job, new Path("/Users/tangjialiang/Desktop/flow/input"));
//		FileOutputFormat.setOutputPath(job, new Path("/Users/tangjialiang/Desktop/flow/output")) ;
		
		boolean res = job.waitForCompletion(true) ;
		System.exit(res?0:1);
	}

}

class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {	
		FlowBean fb = new FlowBean() ;
		String line = value.toString() ;
		String[] words = line.split("\t") ;
		
		String telephone = words[1] ;
		long upFlow = Long.parseLong(words[6]) ;
		long downFlow = Long.parseLong(words[7]) ;
		long totalFlow = Long.parseLong(words[8]) ;
		fb.setFields(telephone, upFlow, downFlow, totalFlow);
		
		context.write(fb, new Text(telephone));
	}
	
}

class FlowReducer extends Reducer<FlowBean, Text, Text, Text> {

	@Override
	protected void reduce(FlowBean key, Iterable<Text> value, Reducer<FlowBean, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Text tel = value.iterator().next() ;
		context.write(new Text(tel), new Text(key.toString()));
	}
	
}
