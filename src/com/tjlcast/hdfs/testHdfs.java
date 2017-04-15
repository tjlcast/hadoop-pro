package com.tjlcast.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class testHdfs {
	FileSystem fs = null ;
	
	@Before
	public void initial() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration() ;
		fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "tangjialiang") ;
	}
	
	@Test
	public void createDir() throws IllegalArgumentException, IOException {
		fs.create(new Path("/tangjialiang/"), true) ;
		fs.close(); 
	}

}
