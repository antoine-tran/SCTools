package de.l3s.streamcorpus.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.umd.cloud9.io.map.HMapSIW;
import tuan.hadoop.conf.JobConfig;
import tuan.terrier.Files;
import tuan.terrier.HadoopDistributedFileSystem;

/**
 * Use Stanford NER to extract the named entities from streamcorpus item. This
 * is useful for example in cases where document contents are processed and only
 * the derived texts are given (i.e. we lost tracks of Serif / Lingpipe tags)
 * 
 * @author tuan
 *
 */
public class StanfordNER extends JobConfig implements Tool {

	private static Logger LOG = LoggerFactory.getLogger(StanfordNER.class);

	private static final String NER_MODEL = "ner";
	private static final String NER_MODEL_PATH = "stanford.ner.model.path";

	private static final class MyMapper 
	extends Mapper<LongWritable, Text, Text, HMapSIW> {

		private AbstractSequenceClassifier<CoreLabel> classifier;
		private Gson gson;
		private InputStream serializedClassifier;
		private final Text keyOut = new Text();
		private final HMapSIW ners = new HMapSIW();

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {

			Configuration conf = context.getConfiguration();

			if (classifier == null) {
				String classifierPath = conf.get(NER_MODEL_PATH);

				FileSystem fs = FileSystem.get(conf);
				FSDataInputStream fdis = fs.open(new Path(classifierPath));				
				serializedClassifier = new GZIPInputStream(fdis);
				try {
					classifier = (AbstractSequenceClassifier<CoreLabel>) CRFClassifier.getClassifier(serializedClassifier);
				} catch (ClassCastException | ClassNotFoundException e) {
					e.printStackTrace();
					throw new IOException("cannot load classifer model");
				}
			}
			gson = new GsonBuilder().create();
		}


		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			if (serializedClassifier != null) {
				serializedClassifier.close();
			}			
		}



		@Override
		protected void map(LongWritable key, Text item, Context context)
				throws IOException, InterruptedException {

			String text = item.toString();
			int i = text.indexOf('\t');

			String docId = text.substring(0,i);
			keyOut.set(docId);
			ners.clear();

			text = text.substring(i+1);
			text = gson.fromJson(text, String.class);

			List<List<CoreLabel>> out = classifier.classify(text);
			for (List<CoreLabel> sentence : out) {

				// get the longest chain of named entity stuff
				String prevType = null;
				StringBuilder sb = new StringBuilder();

				for (CoreLabel word : sentence) {
					// System.out.print(word.word() + '/' + word.get(CoreAnnotations.AnswerAnnotation.class) + ' ');

					String curType = word.get(CoreAnnotations.AnswerAnnotation.class);

					if (curType != prevType) {
						if (sb.length() > 0 && (
								"LOCATION".equals(prevType) ||
								"ORGANIZATION".equals(prevType) ||
								"PERSON".equals(prevType))) {

							int t = convertType(prevType);
							sb.append(t);
							String ne = sb.toString();							
							if (ne.length() > 3 && ne.length() < 30) {
								if (!ners.containsKey(ne)) {
									ners.put(ne, 1);
								} else {
									ners.increment(ne);
								}
							}
						}
						sb.delete(0, sb.length());
						if ("LOCATION".equals(curType) ||"ORGANIZATION".equals(curType) 
								|| "PERSON".equals(curType)) {
							sb.append(word.word());
							sb.append(' ');
						}
					}
					else if ("LOCATION".equals(prevType) ||"ORGANIZATION".equals(prevType) 
							|| "PERSON".equals(prevType)) {
						sb.append(word.word());
						sb.append(' ');
					}
					else {
						sb.delete(0, sb.length());
					}
					prevType = curType;
				}

				if ("LOCATION".equals(prevType) ||"ORGANIZATION".equals(prevType) 
						|| "PERSON".equals(prevType)) {
					int t = convertType(prevType);
					sb.append(t);
					String ne = sb.toString();							
					if (ne.length() > 3 && ne.length() < 30) {
						if (!ners.containsKey(ne)) {
							ners.put(ne, 1);
						} else {
							ners.increment(ne);
						}
					}
				}
			}
			context.write(keyOut, ners);
		}
	}

	// Add extra option about the dexter configuration file path in HDFS,
	// and the disambiguator used
	@SuppressWarnings("static-access")
	@Override
	public Options options() {
		Options opts = super.options();

		Option nerOpt = OptionBuilder.withArgName("classifier-model").hasArg(true)
				.withDescription("Model of Stanford NER classifier")
				.create(NER_MODEL);
		opts.addOption(nerOpt);

		return opts;
	}

	private static int convertType(String type) {
		if ("LOCATION".equals(type)) return 1;
		if ("ORGANIZATION".equals(type)) return 2;
		if ("PERSON".equals(type)) return 3;
		else return -1;
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(args.length);
		Job job = setup(TextInputFormat.class, SequenceFileOutputFormat.class,
				Text.class, HMapSIW.class,//IntFloatArrayListWritable.class,
				Text.class, HMapSIW.class,//IntFloatArrayListWritable.class,
				MyMapper.class, Reducer.class, Reducer.class,
				args);

		// load extra options into configuration object
		String nerConf = null;
		if (!command.hasOption(NER_MODEL)) {
			LOG.error("NER model path is missing");
			return -1;
		}
		nerConf = command.getOptionValue(NER_MODEL);
		job.getConfiguration().set(NER_MODEL_PATH, nerConf);

		// increase heap
		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

		// register the filesystem before starting
		registerHDFS();

		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			LOG.error("Job failed: ", e);
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new StanfordNER(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
