package com.tjlcast.distributedcache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin {

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		String home = "" ;
		
		Configuration conf = new Configuration() ;
		
		Job job = Job.getInstance(conf) ;
		
		job.setJarByClass(MapSideJoin.class);
		
		job.setMapperClass(MapSideJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(home + "input")); 
		FileOutputFormat.setOutputPath(job, new Path(home + "output"));
		
		// 指定需要缓存一个文件到所有的maptask运行节点的工作目录
		// job.addArchiveToClassPath(archive); //缓存jar包到task运行节点的classpath中
		// job.addCacheArchive(uri); // 缓存压缩包到task运行节点的classpath中
		// job.addCacheFile(uri); // 缓存普通文件到task运行节点的工作目录中
		// job.addFileToClassPath(file); // 缓存普通文件到task运行节点的工作目录中
		
		// 将产品文件缓存到task运行节点的工作目录中
		job.addCacheFile(new URI("file://"+home+"exFile/prodcut_info"));
		
		// 设置reduce的数量为0
		job.setNumReduceTasks(0);
		
		boolean res = job.waitForCompletion(true) ;
		System.exit(res?0:1);
	}

}

class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	/**
	 * 使用一个hashMap加载保存productInfo
	 */
	HashMap<String, String> productInfo = new HashMap<String, String>() ;
	Text k = new Text() ;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 从mapper运行的目录中加载缓存的product_info
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("prodcut_info"))) ;
		
		String line ;
		while(StringUtils.isNotEmpty(line=br.readLine())) {
			String[] fields = line.split(",") ;
			productInfo.put(fields[0], fields[1]) ;
		}
	}
	
	/**
	 * map已经获取到product的全部信息。
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String orderLine = key.toString() ;
		String[] fields = orderLine.split("\t") ;
		String pdName = productInfo.get(fields[1]) ;
		k.set(orderLine + "\t" + pdName );
		context.write(k, NullWritable.get());
	}
	
}