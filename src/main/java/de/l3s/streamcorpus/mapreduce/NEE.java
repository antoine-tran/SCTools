package de.l3s.streamcorpus.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ilps.hadoop.StreamItemWritable;
import ilps.hadoop.ThriftFileInputFormat;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.map.MapKI.Entry;
import streamcorpus.EntityType;
import streamcorpus.Sentence;
import streamcorpus.Token;
import tuan.hadoop.conf.JobConfig;

/**
 * Extract named entities from the StreamCorpus at large (grouped by or not by time)
 * */
public class NEE extends JobConfig implements Tool {

	private static final DateTimeFormatter dtf = DateTimeFormat.forPattern("YYYYMMdd");

	private static final String MAPPER_SIZE = "-Xmx1024m"; 

	/** For counting entities globally, use this parameter to store the results */
	private static final String COUNT_ENTITIES_ALL_OPT = "finalout";

	private static final String PHASE = "phase";

	private int phase = 1;

	/** output key = date, output value = map of named entites / word count */
	private static class Phase1Mapper extends Mapper<Text, StreamItemWritable, IntWritable, HMapSIW> {

		private final IntWritable keyOut = new IntWritable();
		private final HMapSIW valOut = new HMapSIW();

		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {
			// This is just to test. The date value can easily be parsed from the file path
			long ts = (long) item.getStream_time().getEpoch_ticks();										
			int dateVal = Integer.parseInt(dtf.print((ts * 1000)));
			keyOut.set(dateVal);
			valOut.clear();

			if (item.getBody() == null) return;

			Map<String, List<Sentence>> sentMap = item.getBody().getSentences();
			if (sentMap == null || sentMap.isEmpty()) return;

			Set<String> keys = sentMap.keySet();
			if (keys == null || keys.isEmpty()) return;

			// Get the first annotated sentence, most likely Serif
			String k = null;
			Iterator<String> keyIter = keys.iterator();
			Set<String> secondNE = new HashSet<>();

			while (keyIter.hasNext()) {
				String annot = keyIter.next();

				for (Sentence s : sentMap.get(annot)) {

					// get the longest chain of named entity stuff
					EntityType et = null;
					StringBuilder sb = new StringBuilder();								
					for (Token t : s.tokens) {	
						if (t.entity_type != et) {
							if (sb.length() > 0 && (
									et == EntityType.ORG ||
									et == EntityType.LOC || 
									et == EntityType.PER ||
									et == EntityType.VEH ||
									et == EntityType.TIME)) {

								String ne = sb.toString();
								if (!valOut.containsKey(ne)) {
									valOut.put(ne, 1);
									if (k != null) {
										secondNE.add(ne);
									}
								}
								else if (!secondNE.contains(ne)) {
									int cnt = valOut.get(ne);
									valOut.put(ne, cnt + 1);
								}
							}
							sb.delete(0, sb.length());
							et = t.entity_type;
							if (et != null) {
								sb.append(t.token);
								sb.append(" ");
							}
						}
						else if (t.entity_type != null) {
							sb.append(t.token);
							sb.append(" ");
						}
						else sb.delete(0, sb.length());
					}
					if (sb.length() > 0 && et != null) {
						System.out.println("\t\t" + sb.toString() + "\t" + et);
					}
				}

				k = annot;
			}
			context.write(keyOut, valOut);
		}
	}

	private static class Phase1Reducer extends Reducer<IntWritable, HMapSIW, IntWritable, HMapSIW> {

		private final HMapSIW valOut = new HMapSIW();

		@Override
		protected void reduce(IntWritable key, Iterable<HMapSIW> cnts, Context context)
				throws IOException, InterruptedException {

			valOut.clear();
			for (HMapSIW cnt : cnts) {
				for (Entry<String> e : cnt.entrySet()) {
					String n = e.getKey();
					if (!valOut.containsKey(n)) {
						valOut.put(n, e.getValue());
					}
					else {
						int c = valOut.get(n);
						valOut.put(n, c + e.getValue());
					}
				}
			}
			context.write(key, valOut);
		}
	}

	/** 
	 * Group all terms for all days and aggregate
	 * Input : StreamItem doc. Output:
	 * key=named entity, value = total count
	 */
	private static class CountAllMapper extends Mapper<IntWritable, HMapSIW, Text, LongWritable> {

		private final Text ne = new Text();
		private final LongWritable cnt = new LongWritable();

		@Override
		protected void map(IntWritable k, HMapSIW cnts, Context context)
				throws IOException, InterruptedException {

			for (Entry<String> e : cnts.entrySet()) {
				ne.set(e.getKey());
				cnt.set(e.getValue());
			}
		}
	}

	@SuppressWarnings("static-access")
	@Override
	public Options options() {
		Options opts = super.options();
		Option outputOpt = OptionBuilder.withArgName("final-output").hasArg(true)
				.withDescription("final output file path (required)")
				.create(COUNT_ENTITIES_ALL_OPT);
		Option phaseOpt = OptionBuilder.withArgName("final-output").hasArg(true)
				.withDescription("final output file path (required)")
				.create(PHASE);
		opts.addOption(outputOpt);
		opts.addOption(phaseOpt);
		return opts;
	}

	private void configureJob(Job job) {
		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx4096m");
		job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");		
	}

	// Count entities by days
	public int phase1() throws IOException {
		Job job = setup(jobName + ": Phase 1", NEE.class,
				input, output,
				ThriftFileInputFormat.class, SequenceFileOutputFormat.class,
				Text.class, HMapSIW.class,
				Text.class, HMapSIW.class,
				Phase1Mapper.class, 
				Phase1Reducer.class, Phase1Reducer.class, reduceNo);

		configureJob(job);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	// check counts of all entities. By this phase, the final output will be used
	public int phase2() throws IOException {
		Job job = setup(jobName + ": Phase 2", NEE.class,
				output, command.getOptionValue(COUNT_ENTITIES_ALL_OPT),
				SequenceFileInputFormat.class, TextOutputFormat.class,
				Text.class, LongWritable.class,
				Text.class, LongWritable.class,
				CountAllMapper.class, 
				LongSumReducer.class, LongSumReducer.class, reduceNo);
		configureJob(job);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		parseOtions(args);
		setMapperSize(MAPPER_SIZE);
		try {
			if (command.hasOption(PHASE)) {
				phase = Integer.parseInt(command.getOptionValue(PHASE));
			}

			if (phase == 1) {
				return phase1();
			}

			else if (phase == 2) {
				return phase2();
			}

			else {
				System.err.println("Unrecognised phase: " + command.getOptionValue(PHASE));
				return -1;
			}
		} catch (NumberFormatException e) {
			
			System.err.println("Unrecognised phase: " + command.getOptionValue(PHASE));
			return -1;
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new NEE(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
