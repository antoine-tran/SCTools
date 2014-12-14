package de.l3s.streamcorpus.mapreduce;

import ilps.hadoop.StreamItemWritable;
import ilps.hadoop.ThriftFileInputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import tuan.hadoop.conf.JobConfig;

public class ExtractURL extends JobConfig implements Tool {

	private static final String MAPPER_SIZE = "-Xmx3078m"; 

	/** output key = docid, output value = URL, json format */
	private static class MyMapper extends Mapper<Text, StreamItemWritable, Text, Text> {

		private final Text keyOut = new Text();
		private final Text valOut = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
		
		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {
			//keyOut.set(dateVal);
			keyOut.set(item.getDoc_id());
			valOut.clear();

			if (item.getBody() == null) return;

			ByteBuffer bf = item.abs_url;
			if (bf != null) {
				byte[] bs = item.abs_url.array();
				valOut.set(bs);
				context.write(keyOut, valOut);
			}			
		}
	}

	private void configureJob(Job job) {
		job.getConfiguration().set("mapreduce.map.memory.mb", "3078");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3078m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");		
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println("Starting");
		parseOtions(args);
		System.out.println("Option passed.");
		setMapperSize(MAPPER_SIZE);
		System.out.println("Mapper size set.");
		Job job = setup(jobName, ExtractTitle.class,
				input, output,
				ThriftFileInputFormat.class, TextOutputFormat.class,
				Text.class, Text.class,
				Text.class, Text.class,
				MyMapper.class, 
				Reducer.class, reduceNo);

		configureJob(job);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new ExtractURL(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
