package com.elex.userAnalyze.genderRecognition.prepareWork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.userAnalyze.genderRecognition.common.Constants;
import com.elex.userAnalyze.genderRecognition.common.HdfsUtils;
import com.elex.userAnalyze.genderRecognition.common.PropertiesUtils;

public class CidUidMappingFileGenerator extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new CidUidMappingFileGenerator(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"CidUidMappingFileGenerator");
		job.setJarByClass(CidUidMappingFileGenerator.class);
 
	    List<Scan> scans = new ArrayList<Scan>(); 
	    Scan hbScan = new Scan();
		hbScan.setStartRow(Bytes.toBytes("lk"));
		hbScan.setStopRow(Bytes.toBytes("ll"));
		hbScan.setCaching(500);
		hbScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("gm_user_action"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("lid"));
		scans.add(hbScan);
		
		TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class,Text.class, Text.class, job);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		String output = PropertiesUtils.getRootDir()+Constants.CUID;
		HdfsUtils.delFile(fs, output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
		
	
	public static class MyMapper extends TableMapper<Text, Text> {
	
		private String cid,uid;
		@Override
		protected void map(ImmutableBytesWritable key, Result r,
				Context context) throws IOException, InterruptedException {
			if (!r.isEmpty()) {
				cid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-2));								
				for (KeyValue kv : r.raw()) {										
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "lid".equals(Bytes.toString(kv.getQualifier()))) {
						uid = Bytes.toString(kv.getValue());
						context.write(new Text(cid), new Text(uid));
					}					
				}
			}
											
		}
					
	}
}
