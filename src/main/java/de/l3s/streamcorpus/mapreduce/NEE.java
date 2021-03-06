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

import de.l3s.lemma.lemma;
import edu.stanford.nlp.ling.tokensregex.MultiWordStringMatcher;
import edu.stanford.nlp.ling.tokensregex.MultiWordStringMatcher.MatchType;
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

	private static final String MAPPER_SIZE = "-Xmx1024m"; 

	/** For counting entities globally, use this parameter to store the results */
	private static final String COUNT_ENTITIES_ALL_OPT = "finalout";

	private static final String PHASE = "phase";

	private int phase = 1;

	/** output key = date, output value = map of named entites / word count */
	private static class Phase1Mapper extends Mapper<Text, StreamItemWritable, Text, HMapSIW> {

		private final Text keyOut = new Text();
		private final HMapSIW valOut = new HMapSIW();

		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {
			//keyOut.set(dateVal);
			keyOut.set(item.getDoc_id());
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


									// ignore too long strings
									if (ne.length() < 50) {
										valOut.put(ne, 1);
										if (k != null) {
											secondNE.add(ne);
										}	
									}
								}
								else if (!secondNE.contains(ne)) {
									if (ne.length() < 50) {
										int cnt = valOut.get(ne);
										valOut.put(ne, cnt + 1);	
									}									
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
						String ne = sb.toString();
						if (ne.length() < 50) {
							if (!valOut.containsKey(ne)) {
								valOut.put(ne, 1);
							}
							else {
								int cnt = valOut.get(ne);
								valOut.put(ne,cnt+1);
							}
						}
					}
				}

				k = annot;
			}
			context.write(keyOut, valOut);
		}
	}

	/** output key = date, output value = map of named entites / types of named entities:
	 * 1 --> Person
	 * 2 --> Location
	 * 3 --> Organization
	 * 4 --> Verhicle
	 * 5 --> Time */
	private static class Phase1TypedMapper extends Mapper<Text, StreamItemWritable, Text, HMapSIW> {

		private final Text keyOut = new Text();
		private final HMapSIW valOut = new HMapSIW();

		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {
			keyOut.set(item.getDoc_id());
			valOut.clear();

			if (item.getBody() == null) return;

			Map<String, List<Sentence>> sentMap = item.getBody().getSentences();
			if (sentMap == null || sentMap.isEmpty()) return;

			Set<String> keys = sentMap.keySet();
			if (keys == null || keys.isEmpty()) return;

			// Get the first annotated sentence, most likely Serif
			Iterator<String> keyIter = keys.iterator();

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

								// ignore too long strings
								if (ne.length() < 50) {
									if (!valOut.containsKey(ne)) {

										if (et == EntityType.VEH)
											valOut.put(ne, 4);
										else if (et == EntityType.ORG)
											valOut.put(ne, 3);
										else if (et == EntityType.LOC)
											valOut.put(ne, 2);
										else if (et == EntityType.PER)
											valOut.put(ne, 1);
									}									
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
						String ne = sb.toString();
						if (ne.length() < 50) {
							if (ne.length() < 50) {
								if (!valOut.containsKey(ne)) {

									if (et == EntityType.VEH)
										valOut.put(ne, 4);
									else if (et == EntityType.ORG)
										valOut.put(ne, 3);
									else if (et == EntityType.LOC)
										valOut.put(ne, 2);
									else if (et == EntityType.PER)
										valOut.put(ne, 1);
								}									
							}
						}
					}
				}
			}
			context.write(keyOut, valOut);
		}
	}

	/** 
	 * Convert all terms to LNRM and build the lemmas for matching against Freebase
	 * Input : StreamItem doc. Output:
	 * key=named entity, value = total count
	 */
	private static class NormalizeMapper extends Mapper<Text, HMapSIW, Text, Text> {

		private final Text keyOut = new Text();
		private final Text valOut = new Text();

		private MultiWordStringMatcher matcher;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			matcher = new MultiWordStringMatcher(MatchType.LNRM);
			lemma.init();
		}

		@Override
		protected void map(Text k, HMapSIW cnts, Context context)
				throws IOException, InterruptedException {

			for (Entry<String> e : cnts.entrySet()) {
				String ent = e.getKey();
				String normalized = matcher.getLnrmRegex(lemma.getLemmatization(ent)).toLowerCase();
				
				int i = -1;
				if (normalized.charAt(i+1) == '('	&& normalized.charAt(i+2) == '?' 
						&& normalized.charAt(i+3) == 'u' && normalized.charAt(i+4) == ')'
						&& normalized.charAt(i+5) == '(' && normalized.charAt(i+6) == '?'
						&& normalized.charAt(i+7) == 'i' && normalized.charAt(i+8) == ')') {
					i = i + 8;
				}
				
				if (i >= 0) {
					normalized = normalized.substring(i+1);
				}
				if (normalized.isEmpty()) {
					return;
				}
				
				keyOut.set(ent);
				valOut.set(normalized);
				context.write(keyOut, valOut);
			}
		}
	}

	/** 
	 * Convert all terms to LNRM and build the lemmas for matching against Freebase, while still keeping
	 * the doc id / entity mapping
	 * Input : StreamItem doc. Output:
	 * key=named entity, value = total count
	 */
	private static class Normalize1Mapper extends Mapper<Text, HMapSIW, Text, Text> {

		private final Text valOut = new Text();

		private MultiWordStringMatcher matcher;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			matcher = new MultiWordStringMatcher(MatchType.LNRM);
			lemma.init();
		}

		@Override
		protected void map(Text k, HMapSIW cnts, Context context)
				throws IOException, InterruptedException {

			for (Entry<String> e : cnts.entrySet()) {
				String ent = e.getKey();
				String normalized = matcher.getLnrmRegex(lemma.getLemmatization(ent)).toLowerCase();
				valOut.set(ent + "\t" + normalized + "\t" + e.getValue());				
				context.write(k, valOut);
			}
		}
	}

	private static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

		private Text valOut = null;

		@Override
		protected void reduce(Text k, Iterable<Text> vals, Context c)
				throws IOException, InterruptedException {

			boolean first = true;

			// emit the first pair and stop
			for (Text v : vals) {
				if (first) {
					valOut = v;
					first = false;
				}
				else break;				
			}

			if (valOut != null)
				c.write(k, valOut);
		}		
	}

	/** 
	 * Group all terms for all days and aggregate
	 * Input : StreamItem doc. Output:
	 * key=named entity, value = total count
	 */
	private static class CountAllMapper extends Mapper<Text, HMapSIW, Text, LongWritable> {

		private final Text ne = new Text();
		private final LongWritable cnt = new LongWritable();

		@Override
		protected void map(Text k, HMapSIW cnts, Context context)
				throws IOException, InterruptedException {

			for (Entry<String> e : cnts.entrySet()) {
				ne.set(e.getKey());
				cnt.set(e.getValue());
				context.write(ne, cnt);
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

	// Count entities by days
	public int phase1variant() throws IOException {
		Job job = setup(jobName + ": Phase 1 (Typed version)", NEE.class,
				input, output,
				ThriftFileInputFormat.class, SequenceFileOutputFormat.class,
				Text.class, HMapSIW.class,
				Text.class, HMapSIW.class,
				Phase1TypedMapper.class, 
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

	// check counts of all entities. By this phase, the final output will be used
	public int phase3() throws IOException {
		Job job = setup(jobName + ": Phase 3", NEE.class,
				output, command.getOptionValue(COUNT_ENTITIES_ALL_OPT),
				SequenceFileInputFormat.class, TextOutputFormat.class,
				Text.class, Text.class,
				Text.class, LongWritable.class,
				NormalizeMapper.class, 
				NormalizeReducer.class, NormalizeReducer.class, reduceNo);
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
	public int phase5() throws IOException {
		Job job = setup(jobName + ": Phase 5", NEE.class,
				output, command.getOptionValue(COUNT_ENTITIES_ALL_OPT),
				SequenceFileInputFormat.class, TextOutputFormat.class,
				Text.class, Text.class,
				Text.class, LongWritable.class,
				Normalize1Mapper.class, 
				Reducer.class, Reducer.class, reduceNo);
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
		System.out.println("Starting");
		parseOtions(args);
		System.out.println("Option passed.");
		setMapperSize(MAPPER_SIZE);
		System.out.println("Mapper size set.");
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

			else if (phase == 3) {
				return phase1variant();
			}

			else if (phase == 4) {
				return phase3();
			}
			
			else if (phase == 5) {
				return phase5();
			}

			else {
				System.err.println("Unrecognised phase: " + command.getOptionValue(PHASE));
				return -1;
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
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
