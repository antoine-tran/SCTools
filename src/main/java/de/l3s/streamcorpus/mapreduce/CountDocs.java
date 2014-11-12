package de.l3s.streamcorpus.mapreduce;

import ilps.hadoop.StreamItemWritable;
import ilps.hadoop.ThriftFileInputFormat;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import streamcorpus.StreamItem;
import tuan.hadoop.conf.JobConfig;

/**
 * Test the StreamCorpus API reader in Hadoop setting. The test counts the number of
 * documents per day from TS14-F dataset
 * @author tuan
 */
public class CountDocs extends JobConfig implements Tool {

	private static final DateTimeFormatter dtf = DateTimeFormat.forPattern("YYYYMMdd");

	// Use integer to represent dates to save memory
	private static final class MyMapper extends Mapper<Text, StreamItemWritable, IntWritable, LongWritable> {

		private final IntWritable keyOut= new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);

		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {

			// This is just to test. The date value can easily be parsed from the file path
			long ts = (long) item.getStream_time().getEpoch_ticks();										
			int dateVal = Integer.parseInt(dtf.print((ts * 1000)));
			keyOut.set(dateVal);
			context.write(keyOut, ONE);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = setup(ThriftFileInputFormat.class, TextOutputFormat.class,
				IntWritable.class, LongWritable.class,
				IntWritable.class, LongWritable.class,
				MyMapper.class, 
				LongSumReducer.class, LongSumReducer.class, 
				args);
		
		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

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
			ToolRunner.run(new CountDocs(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
