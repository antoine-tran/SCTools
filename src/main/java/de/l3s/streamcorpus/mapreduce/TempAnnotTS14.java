/**
 * 
 */
package de.l3s.streamcorpus.mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import ilps.hadoop.StreamItemWritable;
import ilps.hadoop.ThriftFileInputFormat;
import it.cnr.isti.hpc.dexter.StandardTagger;
import it.cnr.isti.hpc.dexter.Tagger;
import it.cnr.isti.hpc.dexter.common.Field;
import it.cnr.isti.hpc.dexter.common.FlatDocument;
import it.cnr.isti.hpc.dexter.common.MultifieldDocument;
import it.cnr.isti.hpc.dexter.disambiguation.Disambiguator;
import it.cnr.isti.hpc.dexter.entity.EntityMatch;
import it.cnr.isti.hpc.dexter.entity.EntityMatchList;

import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedDocument;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;
import it.cnr.isti.hpc.dexter.rest.domain.Tagmeta;
import it.cnr.isti.hpc.dexter.spotter.Spotter;
import it.cnr.isti.hpc.dexter.util.DexterLocalParams;
import it.cnr.isti.hpc.dexter.util.DexterParams;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tuan.io.FileUtility;
import tuan.terrier.Files;
import tuan.terrier.HadoopDistributedFileSystem;

import java.lang.StringBuilder;

/**
 * Annotate the StreamCorpus dataset with Dexter
 * @author tuan
 *
 */
public class TempAnnotTS14 extends Configured implements Tool {

	private static Logger LOG = LoggerFactory.getLogger(TempAnnotTS14.class);
	
	private static final String DISAMB_OPT = "ned";
	private static final String DISAMB_HDFS_OPT = "dexter.disambiguator";
	
	private static final String DEXTER_CONF_OPT = "dexter";
	private static final String DEXTER_CONF_PATH_HDFS = "dexter-conf.path";
	
	// private static final IdHelper helper = IdHelperFactory.getStdIdHelper();

	public static enum Version {
		HADOOP_1,
		HADOOP_2
	}

	private Version version = Version.HADOOP_2;

	private String mapperSize = "-Xmx1024m"; 

	public static final String INPUT_OPT = "in";
	public static final String OUTPUT_OPT = "out";
	public static final String REDUCE_NO = "reduce";
	public static final String JOB_NAME = "name";
	public static final String REMOVE_OUTPUT = "rmo";
	public static final String COMPRESS_OPT = "compress";
	protected CommandLine command;	
	
	protected int reduceNo = 24;
	protected String jobName;
	protected String input;
	protected String output;
	protected String compressType = null;	
	protected boolean removeOutputDirectory = false;
	
	private static final class MyMapper 
			extends Mapper<Text, StreamItemWritable, Text, Text> {

		private final Text keyOut = new Text();
		private final Text neds = new Text();
		
		// Each JVM has only one dexterParam instance
		private DexterParams dexterParams;
		private Spotter s;
		private Disambiguator d;
		private Tagger tagger;
		private boolean addWikinames;
		private Integer entitiesToAnnotate;
		private double minConfidence;
		
		private String ned;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			
			// register HDFS for each JVM in the datanode
			if (!Files.hasFileSystemScheme(HadoopDistributedFileSystem.HDFS_SCHEME)) {
				Files.addFileSystemCapability(new HadoopDistributedFileSystem(conf));	
			}
			
			if (dexterParams == null) {
				String dexterConf = System.getProperty("conf");
				if (dexterConf == null) {
					dexterConf = conf.get(DEXTER_CONF_PATH_HDFS);
					System.setProperty("conf", dexterConf);
				}
				dexterParams = DexterParams.getInstance();
			}
			s = dexterParams.getSpotter("wiki-dictionary");
			d = dexterParams.getDisambiguator("wikiminer");
			
			tagger = new StandardTagger("std", s, d);
			addWikinames = new Boolean("true");

			entitiesToAnnotate = new Integer(50);
			minConfidence = Double.parseDouble("0.1");
			
			if (ned == null) {
				ned = conf.get(DISAMB_HDFS_OPT);
			}
		}
		
		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {
			String docId = item.getDoc_id();
			keyOut.set(docId);
			// neds.clear();
			
			if (item.getBody() == null || item.getBody().getClean_visible() == null ||
					item.getBody().getClean_visible().isEmpty()) {
				return;
			}
			
			String text = item.getBody().getClean_visible();
			MultifieldDocument doc = parseDocument(text, "text");		

			EntityMatchList eml = tagger.tag(new DexterLocalParams(), doc);

			AnnotatedDocument adoc = new AnnotatedDocument(doc);

			
			annotate(adoc, eml, entitiesToAnnotate, addWikinames, minConfidence);
			
      StringBuilder sb = new StringBuilder();
			for (AnnotatedSpot spot : adoc.getSpots()) {
        sb.append("\t");
        sb.append(spot.getEntity());
        sb.append(",");
        sb.append(spot.getScore());
				// System.out.println("Mention: " + spot.getMention() 
				// + ", wikipedia page ID: " + spot.getEntity() + ", score: " + spot.getScore());
				// neds.add(spot.getEntity(), (float) spot.getScore());
			}
			neds.set(sb.toString());
			context.write(keyOut, neds);
		}		
	}

	@Override
	public int run(String[] args) throws Exception {
 		System.out.println(args.length);
    Job job = setup(ThriftFileInputFormat.class, TextOutputFormat.class,
				Text.class, Text.class,//IntFloatArrayListWritable.class,
				Text.class, Text.class,//IntFloatArrayListWritable.class,
				MyMapper.class, Reducer.class, 
				args);
		
		// load extra options into configuration object
		String dexterConf = null;
		if (!command.hasOption(DEXTER_CONF_OPT)) {
			LOG.error("Dexter configuration path missing");
			return -1;
		}
		dexterConf = command.getOptionValue(DEXTER_CONF_OPT);
		job.getConfiguration().set(DEXTER_CONF_PATH_HDFS, dexterConf);
		
		String disambiguator = "tagme";
		if (command.hasOption(DISAMB_OPT)) {
			disambiguator = command.getOptionValue(DISAMB_OPT);
		}
		job.getConfiguration().set(DISAMB_HDFS_OPT, disambiguator);
		
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

	public static MultifieldDocument parseDocument(String text, String format) {
		Tagmeta.DocumentFormat df = Tagmeta.getDocumentFormat(format);
		MultifieldDocument doc = null;
		if (df == Tagmeta.DocumentFormat.TEXT) {
			doc = new FlatDocument(text);
		}
		return doc;
	}
	
	public static void annotate(AnnotatedDocument adoc, EntityMatchList eml,
			int nEntities, boolean addWikiNames, double minConfidence) {
		eml.sort();
		EntityMatchList emlSub = new EntityMatchList();
		int size = Math.min(nEntities, eml.size());
		List<AnnotatedSpot> spots = adoc.getSpots();
		spots.clear();
		for (int i = 0; i < size; i++) {
			EntityMatch em = eml.get(i);
			if (em.getScore() < minConfidence) {
				
				continue;
			}
			emlSub.add(em);
			AnnotatedSpot spot = new AnnotatedSpot(em.getMention(),
					em.getSpotLinkProbability(), em.getStart(), em.getEnd(), em
							.getSpot().getLinkFrequency(), em.getSpot()
							.getFrequency(), em.getId(), em.getFrequency(),
					em.getCommonness(), em.getScore());
			spot.setField(em.getSpot().getField().getName());
			/*if (addWikiNames) {
				spot.setWikiname(helper.getLabel(em.getId()));
			}*/

			spots.add(spot);
		}
		MultifieldDocument annotatedDocument = getAnnotatedDocument(adoc,
				emlSub);
		adoc.setAnnotatedDocument(annotatedDocument);
	}
	
	public static MultifieldDocument getAnnotatedDocument(AnnotatedDocument adoc,
			EntityMatchList eml) {
		Collections.sort(eml, new EntityMatch.SortByPosition());

		Iterator<Field> iterator = adoc.getDocument().getFields();
		MultifieldDocument annotated = new MultifieldDocument();
		while (iterator.hasNext()) {
			int pos = 0;
			StringBuffer sb = new StringBuffer();
			Field field = iterator.next();
			String currentField = field.getName();
			String currentText = field.getValue();

			for (EntityMatch em : eml) {
				if (!em.getSpot().getField().getName().equals(currentField)) {
					continue;
				}
				assert em.getStart() >= 0;
				assert em.getEnd() >= 0;
				try {
					sb.append(currentText.substring(pos, em.getStart()));
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					LOG.warn(
							"error annotating text output of bound for range {} - {} ",
							pos, em.getStart());
					LOG.warn("text: \n\n {}\n\n", currentText);
				}
				// the spot has been normalized, i want to retrieve the real one
				String realSpot = "none";
				try {
					realSpot = currentText
							.substring(em.getStart(), em.getEnd());
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					LOG.warn(
							"error annotating text output of bound for range {} - {} ",
							pos, em.getStart());
					LOG.warn("text: \n\n {}\n\n", currentText);
				}
				sb.append(
						"<a href=\"#\" onmouseover='manage(" + em.getId()
								+ ")' >").append(realSpot).append("</a>");
				pos = em.getEnd();
			}
			if (pos < currentText.length()) {
				try {
					sb.append(currentText.substring(pos));
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					LOG.warn(
							"error annotating text output of bound for range {} - end ",
							pos);
					LOG.warn("text: \n\n {}\n\n", currentText);
				}

			}
			annotated.addField(new Field(field.getName(), sb.toString()));

		}

		return annotated;
	}
	
	@SuppressWarnings("static-access")
	public Options options() {
		Options opts = new Options();

		Option jnameOpt = OptionBuilder.withArgName("job-name").hasArg(true)
				.withDescription("Job name")
				.create(JOB_NAME);

		Option inputOpt = OptionBuilder.withArgName("input-path").hasArg(true)
				.withDescription("input file / directory path (required)")
				.create(INPUT_OPT);

		Option outputOpt = OptionBuilder.withArgName("output-path").hasArg(true)
				.withDescription("output file path (required)")
				.create(OUTPUT_OPT);

		Option reduceOpt = OptionBuilder.withArgName("reduce-no").hasArg(true)
				.withDescription("number of reducer nodes").create(REDUCE_NO);

		Option rmOpt = OptionBuilder.withArgName("remove-out").hasArg(false)
				.withDescription("remove the output then create again before writing files onto it")
				.create(REMOVE_OUTPUT);

		Option cOpt = OptionBuilder.withArgName("compress-option").hasArg(true)
				.withDescription("compression option").create(COMPRESS_OPT);

		opts.addOption(jnameOpt);
		opts.addOption(inputOpt);
		opts.addOption(reduceOpt);
		opts.addOption(outputOpt);
		opts.addOption(rmOpt);
		opts.addOption(cOpt);
		
		Option nedOpt = OptionBuilder.withArgName("disambiguator").hasArg(true)
				.withDescription("method of disambiguation (Tagme / Wikiminer)")
				.create(DISAMB_OPT);
		opts.addOption(nedOpt);
		
		Option dexterOpt = OptionBuilder.withArgName("dexterconfiguration").hasArg(true)
				.withDescription("path to dexter configuration file")
				.create(DEXTER_CONF_OPT);
		opts.addOption(dexterOpt);
		return opts;
		
	}
	
	public int parseOtions(String[] args) {
		Options opts = options();
		CommandLineParser parser = new GnuParser();
		try {
			command = parser.parse(opts, args);
		} catch (ParseException e) {
			System.err.println("Error parsing command line: " + e.getMessage());
			return -1;
		}

		if (!command.hasOption(INPUT_OPT) || !command.hasOption(OUTPUT_OPT)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(getClass().getName(), opts);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		
		if (command.hasOption(REDUCE_NO)) {
			try {
				reduceNo = Integer.parseInt(command.getOptionValue(REDUCE_NO));
			} catch (NumberFormatException e) {
				System.err.println("Error parsing reducer number: "
						+ e.getMessage());
			}
		}
		
		if (command.hasOption(JOB_NAME)) {
			jobName = command.getOptionValue(JOB_NAME);
			jobName = jobName.replace('-',' ');
		} else {
			jobName = this.getClass().getCanonicalName();
		}
		
		if (command.hasOption(REMOVE_OUTPUT)) {
			markOutputForDeletion();
		}

		if (command.hasOption(COMPRESS_OPT)) {
			setCompress(command.getOptionValue(COMPRESS_OPT));
		}

		input = command.getOptionValue(INPUT_OPT);
		output = command.getOptionValue(OUTPUT_OPT);
		
		return 0;
	}
	
	public void markOutputForDeletion() {
		removeOutputDirectory = true;
	}

	public void setMapperSize(String mapSize) {
		mapperSize = mapSize;
	}

	/**
	 * Compress type: gz, bz2, lz4, snappy, lzo
	 * @param type
	 */
	public void setCompress(String type) {
		compressType = type;
	}

	/**
	 * A simple job registration without any configuration
	 * @throws IOException 
	 */
	public <JOB> Job create(String jobName, Class<JOB> jobClass) throws IOException {
		// Hadoop 2.0
		Job job;
		if (version == Version.HADOOP_2) {
			job = Job.getInstance(getConf());
			job.setJobName(jobName);
			
			// This is the nasty thing in MapReduce v2 and YARN: 
			// They always prefer their ancient jars first. 
			// Set this on to say you don't like it
			job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
		}

		// Hadoop 1.x
		else {
			job = new Job(getConf(), jobName);
		}
		
		job.setJarByClass(jobClass);
		
		return job;
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	public <JOB, INFILE extends InputFormat, OUTFILE extends OutputFormat,
	KEYIN, VALUEIN, KEYOUT, VALUEOUT, 
	MAPPER extends Mapper, REDUCER extends Reducer>
	Job setup(Class<INFILE> inputFormatClass,
			Class<OUTFILE> outputFormatClass,
			Class<KEYIN> mapKeyOutClass,
			Class<VALUEIN> mapValOutClass,
			Class<KEYOUT> keyOutClass,
			Class<VALUEOUT> valOutClass,
			Class<REDUCER> reduceClass, String[] args) throws IOException {
		parseOtions(args);
		return setup(jobName, this.getClass(),
				input, output, inputFormatClass, outputFormatClass,
				mapKeyOutClass, mapValOutClass, keyOutClass, valOutClass,
				Mapper.class, reduceClass, reduceNo);
	}
	
	@SuppressWarnings({ "rawtypes", "deprecation" })
	public <JOB, INFILE extends InputFormat, OUTFILE extends OutputFormat,
	KEYIN, VALUEIN, KEYOUT, VALUEOUT, 
	MAPPER extends Mapper, REDUCER extends Reducer, COMBINER extends Reducer>
	Job setup(
			Class<INFILE> inputFormatClass,
			Class<OUTFILE> outputFormatClass,
			Class<KEYIN> mapKeyOutClass,
			Class<VALUEIN> mapValOutClass,
			Class<KEYOUT> keyOutClass,
			Class<VALUEOUT> valOutClass,
			Class<MAPPER> mapClass,
			Class<REDUCER> reduceClass,
			Class<COMBINER> combinerClass,
			String[] args) throws IOException {
		parseOtions(args);
		return setup(jobName, this.getClass(),
				input, output, inputFormatClass, outputFormatClass,
				mapKeyOutClass, mapValOutClass, keyOutClass, valOutClass,
				mapClass, reduceClass, combinerClass, reduceNo);
	}
	
	@SuppressWarnings({ "rawtypes", "deprecation" })
	public <JOB, INFILE extends InputFormat, OUTFILE extends OutputFormat,
	KEYIN, VALUEIN, KEYOUT, VALUEOUT, 
	MAPPER extends Mapper, REDUCER extends Reducer>
	Job setup(
			Class<INFILE> inputFormatClass,
			Class<OUTFILE> outputFormatClass,
			Class<KEYIN> mapKeyOutClass,
			Class<VALUEIN> mapValOutClass,
			Class<KEYOUT> keyOutClass,
			Class<VALUEOUT> valOutClass,
			Class<MAPPER> mapClass,
			Class<REDUCER> reduceClass,
			String[] args) throws IOException {
		parseOtions(args);
		return setup(jobName, this.getClass(),
				input, output, inputFormatClass, outputFormatClass,
				mapKeyOutClass, mapValOutClass, keyOutClass, valOutClass,
				mapClass, reduceClass, reduceNo);
	}
	
	@SuppressWarnings({ "rawtypes", "deprecation" })
	public <JOB, INFILE extends InputFormat, OUTFILE extends OutputFormat,
	KEYIN, VALUEIN, KEYOUT, VALUEOUT, 
	MAPPER extends Mapper, REDUCER extends Reducer>
	Job setup(
			String jobName,	Class<JOB> jobClass, 
			String inpath, String outpath,
			Class<INFILE> inputFormatClass,
			Class<OUTFILE> outputFormatClass,
			Class<KEYIN> mapKeyOutClass,
			Class<VALUEIN> mapValOutClass,
			Class<KEYOUT> keyOutClass,
			Class<VALUEOUT> valOutClass,
			Class<REDUCER> reduceClass,
			int reduceNo) throws IOException {

		Job job = setup(jobName, jobClass,
				inpath, outpath, inputFormatClass, outputFormatClass,
				mapKeyOutClass, mapValOutClass, keyOutClass, valOutClass,
				Mapper.class, reduceClass, reduceNo);

		return job;
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	public <JOB, INFILE extends InputFormat, OUTFILE extends OutputFormat,
	KEYIN, VALUEIN, KEYOUT, VALUEOUT, 
	MAPPER extends Mapper, REDUCER extends Reducer>
	Job setup(
			String jobName,	Class<JOB> jobClass, 
			String inpath, String outpath,
			Class<INFILE> inputFormatClass,
			Class<OUTFILE> outputFormatClass,
			Class<KEYIN> mapKeyOutClass,
			Class<VALUEIN> mapValOutClass,
			Class<KEYOUT> keyOutClass,
			Class<VALUEOUT> valOutClass,
			Class<MAPPER> mapClass,
			Class<REDUCER> reduceClass,
			int reduceNo) throws IOException {

		Job job = create(jobName, jobClass);

		// Common configurations
		job.getConfiguration().setBoolean(
				"mapreduce.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapreduce.reduce.tasks.speculative.execution", false);


		// Option: Java heap space
		job.getConfiguration().set("mapreduce.child.java.opts", mapperSize);
		job.getConfiguration().set("mapred.child.java.opts", mapperSize);

		setCompressOption(job);

		job.setNumReduceTasks(reduceNo);

		for (String line : FileUtility.readLines(inpath)) {
			Path ip = new Path(line);
			FileInputFormat.addInputPath(job, ip);
		}
		
		Path op = new Path(outpath);

		if (removeOutputDirectory) {
			FileSystem fs = FileSystem.get(getConf());
			fs.delete(op, true);
		}

		FileOutputFormat.setOutputPath(job, op);

		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(outputFormatClass);

		job.setMapOutputKeyClass(mapKeyOutClass);
		job.setMapOutputValueClass(mapValOutClass);

		job.setOutputKeyClass(keyOutClass);
		job.setOutputValueClass(valOutClass);

		job.setMapperClass(mapClass);
		job.setReducerClass(reduceClass);

		return job;
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	public <JOB, INFILE extends InputFormat, OUTFILE extends OutputFormat,
	KEYIN, VALUEIN, KEYOUT, VALUEOUT, 
	MAPPER extends Mapper, REDUCER extends Reducer, COMBINER extends Reducer>
	Job setup(
			String jobName,	Class<JOB> jobClass, 
			String inpath, String outpath,
			Class<INFILE> inputFormatClass,
			Class<OUTFILE> outputFormatClass,
			Class<KEYIN> mapKeyOutClass,
			Class<VALUEIN> mapValOutClass,
			Class<KEYOUT> keyOutClass,
			Class<VALUEOUT> valOutClass,
			Class<MAPPER> mapClass,
			Class<REDUCER> reduceClass,
			Class<COMBINER> combinerClass,
			int reduceNo) throws IOException {

		Job job = setup(jobName, jobClass,
				inpath, outpath, inputFormatClass, outputFormatClass,
				mapKeyOutClass, mapValOutClass, keyOutClass, valOutClass,
				mapClass, reduceClass, reduceNo);

		job.setCombinerClass(combinerClass);

		setCompressOption(job);

		return job;
	}

	public void setCompressOption(Job job) {
		// Option: compress output
		if (compressType != null) {
			job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", true);
			job.getConfiguration().setBoolean("mapred.output.compress", true);

			job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.type", "BLOCK"); 
			job.getConfiguration().set("mapred.output.compression.type", "BLOCK"); 

			job.getConfiguration().setBoolean("mapred.compress.map.output", true); 
			job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);

			if ("bz2".equals(compressType)) {
				getConf().setClass("mapreduce.output.fileoutputformat.compress.codec", 
						BZip2Codec.class, CompressionCodec.class);
				getConf().setClass("mapred.output.compression.codec", 
						BZip2Codec.class, CompressionCodec.class);

				getConf().setClass("mapred.map.output.compression.codec", 
						BZip2Codec.class, CompressionCodec.class);
				getConf().setClass("mapreduce.map.output.compress.codec", 
						BZip2Codec.class, CompressionCodec.class);
			}			
			else if ("gz".equals(compressType)) {
				getConf().setClass("mapreduce.output.fileoutputformat.compress.codec", 
						GzipCodec.class, CompressionCodec.class);
				getConf().setClass("mapred.output.compression.codec", 
						GzipCodec.class, CompressionCodec.class);

				getConf().setClass("mapred.map.output.compression.codec", 
						GzipCodec.class, CompressionCodec.class);
				getConf().setClass("mapreduce.map.output.compress.codec", 
						GzipCodec.class, CompressionCodec.class);
			}
			else if ("lz4".equals(compressType)) {
				getConf().setClass("mapreduce.output.fileoutputformat.compress.codec", 
						Lz4Codec.class, CompressionCodec.class);
				getConf().setClass("mapred.output.compression.codec", 
						Lz4Codec.class, CompressionCodec.class);

				getConf().setClass("mapred.map.output.compression.codec", 
						Lz4Codec.class, CompressionCodec.class);
				getConf().setClass("mapreduce.map.output.compress.codec", 
						Lz4Codec.class, CompressionCodec.class);
			}
			else if ("snappy".equals(compressType)) {
				getConf().setClass("mapreduce.output.fileoutputformat.compress.codec", 
						SnappyCodec.class, CompressionCodec.class);
				getConf().setClass("mapred.output.compression.codec", 
						SnappyCodec.class, CompressionCodec.class);

				getConf().setClass("mapred.map.output.compression.codec", 
						SnappyCodec.class, CompressionCodec.class);
				getConf().setClass("mapreduce.map.output.compress.codec", 
						SnappyCodec.class, CompressionCodec.class);
			}
			else if ("lzo".equals(compressType)) {
				getConf().set("mapreduce.output.fileoutputformat.compress.codec", 
						"com.hadoop.compression.lzo.LzoCodec");
				getConf().set("mapred.output.compression.codec", 
						"com.hadoop.compression.lzo.LzoCodec");

				getConf().set("mapred.map.output.compression.codec", 
						"com.hadoop.compression.lzo.LzoCodec");
				getConf().set("mapreduce.map.output.compress.codec", 
						"com.hadoop.compression.lzo.LzoCodec");
			}
			else throw new RuntimeException("Unknown compress codec: " + compressType);
		}
	}

	public void setVersion(Version v) {
		this.version = v;
	}
	
	public Version getVersion() {
		return version;
	}
	
	/** Register the HDFS filesystem to tuan.terrier.Files, so that any libraries use the class
	 * to open files can both read in local and HDFS file systems 
	 * @throws IOException */
	public void registerHDFS() throws IOException {
		if (!Files.hasFileSystemScheme(HadoopDistributedFileSystem.HDFS_SCHEME)) {
			Files.addFileSystemCapability(new HadoopDistributedFileSystem(getConf()));	
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
    for (String a : args) System.out.println(a);
		try {
      System.out.println(args.length);
			ToolRunner.run(new TempAnnotTS14(), args);
		} catch (Exception e) {
			LOG.error("FAILED: ", e);
			e.printStackTrace();
		}
	}
}
