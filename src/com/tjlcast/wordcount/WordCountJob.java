package com.tjlcast.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "167.88.124.154");
		Job job = Job.getInstance(conf) ;
		
		job.setJarByClass(WordCountJob.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("/tangjialiang/hadoop/hdfs/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("/tangjialiang/hadoop/hdfs/wordcount/output"));
		
		boolean waitForCompletion = job.waitForCompletion(true) ;
		System.exit(waitForCompletion?0:1);
	}
}	
