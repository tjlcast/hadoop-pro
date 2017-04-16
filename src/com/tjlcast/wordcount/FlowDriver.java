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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FlowDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String home = "/Users/tangjialiang/Desktop/project/bigdata/cloud/mr/flow/" ;
		
		Configuration conf = new Configuration() ;
		System.setProperty("HADOOP_USER_NAME", "root"); // set jvm user_name
		conf.set("fs.defaultFS", "hdfs://10.108.217.142:9000"); // remote need
		conf.set("mapreduce.framework.name", "yarn") ; // remote need
		conf.set("yarn.resourcemanager.hostname", "10.108.217.142"); // remote need
		
		Job job = Job.getInstance(conf) ;
		job.setJar(home+"flow.jar"); // remote need
//		job.setJarByClass(FlowDriver.class); // local need and at remote operate
		
		// set Parition 
		job.setPartitionerClass(TelephoneParition.class);
		// the number of reduce tasks is 1
		job.setNumReduceTasks(2);
		
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
//		boolean exists = fs.exists(new Path(home + "output")) ;
//		if(exists) fs.delete(new Path(home + "output"), true) ;
//		FileInputFormat.setInputPaths(job, new Path(home + "input"));
//		FileOutputFormat.setOutputPath(job, new Path(home + "output")) ;
		
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

class TelephoneParition extends HashPartitioner<FlowBean, Text> {

	@Override
	public int getPartition(FlowBean key, Text value, int numReduceTasks) {
		String telephone = key.getTelephone() ;
		if (telephone==null ||  telephone.equals("")) {
			return 0 ;
		}
		int flag = Integer.parseInt(telephone.substring(telephone.length()-1, telephone.length())) ;
		return (flag%2) ;
	}
}
