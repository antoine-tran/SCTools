/**
 * 
 */
package de.l3s.streamcorpus.mapreduce;

import ilps.hadoop.StreamItemWritable;
import ilps.hadoop.ThriftFileInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tuan.hadoop.conf.JobConfig;

/**
 * @author tuan
 *
 */
public class TestText extends JobConfig implements Tool {

	// Use integer to represent dates to save memory
	private static final class MyMapper extends Mapper<Text, StreamItemWritable, Text, Text> {

		private final Text keyOut= new Text();
		private final Text ONE = new Text();

		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {

			// This is just to test. The date value can easily be parsed from the file path
			keyOut.set(item.getDoc_id());
			if (item.getBody() == null || item.getBody().getClean_visible() == null ||
					item.getBody().getClean_visible().isEmpty()) {
				return;
			}
			ONE.set(item.getBody().getClean_visible());
			context.write(keyOut, ONE);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = setup(ThriftFileInputFormat.class, TextOutputFormat.class,
				Text.class, Text.class,
				Text.class, Text.class,
				MyMapper.class, Reducer.class, 
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
			ToolRunner.run(new TestText(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
