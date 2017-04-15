package com.tjlcast.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
		conf.set("mapreduce.framework.name", "yarn") ;
		conf.set("yarn.resourcemanager.hostname", "10.108.217.142");
		
		Job job = Job.getInstance(conf) ;
//		job.setJar("");
		job.setJarByClass(FlowDriver.class);
		
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(FlowReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(""));
		FileOutputFormat.setOutputPath(job, new Path(""));
		
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
		long upFlow = Long.parseLong(words[5]) ;
		long downFlow = Long.parseLong(words[6]) ;
		long totalFlow = Long.parseLong(words[7]) ;
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
