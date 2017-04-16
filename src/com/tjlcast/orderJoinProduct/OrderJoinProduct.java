package com.tjlcast.orderJoinProduct;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OrderJoinProduct {
	//t_order:id,date,pid,amount|1001,20150710,P00001,2
	//t_product:id,name,category_id,price|P00001,小米5，1000，2
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration() ;
		System.setProperty("HADOOP_USER_NAME", "root"); // set jvm user_name
		conf.set("fs.defaultFS", "hdfs://10.108.217.142:9000"); // remote need
		conf.set("mapreduce.framework.name", "yarn") ; // remote need
		conf.set("yarn.resourcemanager.hostname", "10.108.217.142"); // remote need
		
		Job job = Job.getInstance(conf) ;
		job.setJar("/Users/tangjialiang/Desktop/project/bigdata/cloud/mr/OrderJoinProduct/OrderJoinProduct.jar"); // remote need
//		job.setJarByClass(OrderJoinProduct.class); // local need and at remote operate
		
		// set Parition 
//		job.setPartitionerClass(TelephoneParition.class);
		// the number of reduce tasks is 1
//		job.setNumReduceTasks(2);
		
		job.setMapperClass(OPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BeanInfo.class);
		
		job.setReducerClass(OPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// remote need
		FileSystem fs = FileSystem.get(conf);
		boolean exists = fs.exists(new Path("/tangjialiang/mr/orderjoinproduct/output")) ;
		if(exists) fs.delete(new Path("/tangjialiang/mr/orderjoinproduct/output"), true) ;
		FileInputFormat.setInputPaths(job, new Path("/tangjialiang/mr/orderjoinproduct/input"));
		FileOutputFormat.setOutputPath(job, new Path("/tangjialiang/mr/orderjoinproduct/output"));
		
		// local need
//		FileSystem fs = FileSystem.get(conf);
//		boolean exists = fs.exists(new Path("/Users/tangjialiang/Desktop/project/bigdata/cloud/mr/OrderJoinProduct/output")) ;
//		if(exists) fs.delete(new Path("/Users/tangjialiang/Desktop/project/bigdata/cloud/mr/OrderJoinProduct/output"), true) ;
//		FileInputFormat.setInputPaths(job, new Path("/Users/tangjialiang/Desktop/project/bigdata/cloud/mr/OrderJoinProduct/input"));
//		FileOutputFormat.setOutputPath(job, new Path("/Users/tangjialiang/Desktop/project/bigdata/cloud/mr/OrderJoinProduct/output")) ;
		
		boolean res = job.waitForCompletion(true) ;
		System.exit(res?0:1);
	}
	
}

class OPMapper extends Mapper<LongWritable, Text, Text, BeanInfo> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, BeanInfo>.Context context)
			throws IOException, InterruptedException {
		//t_order:id,date,pid,amount|1001,20150710,P00001,2
		//t_product:id,name,category_id,price|P00001,小米5，1000，2
		
		String line = value.toString() ;
		BeanInfo bean = new BeanInfo() ;
		Text k = new Text() ;
		
		FileSplit fs = (FileSplit)context.getInputSplit() ;
		String fname = fs.getPath().getName() ;
		
		if (fname.startsWith("order")) {
			String[] words = line.split(",");
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");//小写的mm表示的是分钟    
			Date date = null;
			try {
				date = sdf.parse(words[1]);
			} catch (ParseException e) {
				System.out.println(e);
			}  
			bean.setOrderInfo(words[0], date, words[2], Integer.parseInt(words[3]));
			k.set(words[2]);
		} else {
			String[] words = line.split(",") ;
			bean.setProductInfo(words[0], words[1], words[2], Float.parseFloat(words[3]));
			k.set(words[0]);
		}
		
		context.write(k, bean); 
	}
	
}

class OPReducer extends Reducer<Text, BeanInfo, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<BeanInfo> values,
			Reducer<Text, BeanInfo, Text, Text>.Context context) throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder() ;
		Text retK = new Text();
		Text retV = new Text() ;
		
		for(BeanInfo bean : values) {
			if (!bean.isOrder()) {
				retK.set(bean.toString()) ;
			} else {
				sb.append(bean.toString()) ;
			}
		}
		retV.set(sb.toString());
		
		context.write(retK, retV);
	}
	
}
