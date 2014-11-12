/**
 * 
 */
package de.l3s.streamcorpus.mapreduce;


import java.io.IOException;
import java.util.HashSet;

import ilps.hadoop.StreamItemWritable;
import it.cnr.isti.hpc.dexter.hadoop.AnnotateMapper;
import it.cnr.isti.hpc.dexter.hadoop.HadoopAnnotation;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tuan.hadoop.io.IntFloatArrayListWritable;


/**
 * Annotate the StreamCorpus dataset with Dexter
 * @author tuan
 *
 */
public class AnnotTS14 extends HadoopAnnotation implements Tool {

	private static Logger LOG = LoggerFactory.getLogger(AnnotTS14.class);

	private static final class MyMapper 
			extends AnnotateMapper<Text, StreamItemWritable, 
			Text, IntFloatArrayListWritable> {

		@Override
		public Text instantiateKeyOutput() {
			return new Text();
		}

		@Override
		public IntFloatArrayListWritable instantiateValueOutput() {
			return new IntFloatArrayListWritable();
		}

		@Override
		public void preAnnotations(Text keyIn, StreamItemWritable item,
				Text keyOut, IntFloatArrayListWritable valOut) {
			String docId = item.getDoc_id();
			keyOut.set(docId);
			valOut.clear();
		}	

		@Override
		public void consumeAnnotation(Text k,
				IntFloatArrayListWritable neds, AnnotatedSpot spot) {
			neds.add(spot.getEntity(), (float) spot.getScore());
		}

		@Override
		public Iterable<String> contents(StreamItemWritable item) {		
			if (item.getBody() == null || item.getBody().getClean_visible() == null ||
					item.getBody().getClean_visible().isEmpty()) {
				return null;
			}
			HashSet<String> values = new HashSet<>();
			values.add(item.getBody().getClean_visible());
			return values;
		}
	}

	@SuppressWarnings("unchecked")
	public Job setup(String[] args) throws IOException, ClassNotFoundException {
		Job job = super.setup(args);
		
		// increase heap
		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
		
		return job;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new AnnotTS14(), args);
		} catch (Exception e) {
			LOG.error("FAILED: ", e);
			e.printStackTrace();
		}
	}
}
