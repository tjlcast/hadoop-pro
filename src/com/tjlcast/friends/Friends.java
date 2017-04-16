package com.tjlcast.friends;

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

public class Friends {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String home = "" ;
		Configuration conf = new Configuration() ;
		
		Job job = Job.getInstance(conf) ;
		job.setJarByClass(Friends.class);
		
		job.setMapperClass(FriendsMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(FriendsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(home + "input"));
		FileOutputFormat.setOutputPath(job, new Path(home + "output"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		 
	}

}

class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
	// input: A:B,C,D,E
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = key.toString() ;
		String[] person_friends = line.split(":") ;
		for(String friend : person_friends[1].split(",")) {
			context.write(new Text(friend), new Text(person_friends[0]));
		}
	}
	
}

class FriendsReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text friend, Iterable<Text> persons, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder() ;
		for(Text person : persons) {
			sb.append(person.toString()).append(",") ;
		}
		
		context.write(friend, new Text(sb.toString()));
	}
	
}