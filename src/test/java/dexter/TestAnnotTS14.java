package dexter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import it.cnr.isti.hpc.dexter.StandardTagger;
import it.cnr.isti.hpc.dexter.Tagger;
import it.cnr.isti.hpc.dexter.common.MultifieldDocument;
import it.cnr.isti.hpc.dexter.disambiguation.Disambiguator;
import it.cnr.isti.hpc.dexter.entity.EntityMatchList;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedDocument;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;
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
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.l3s.streamcorpus.mapreduce.AnnotTS14;

import streamcorpus.StreamItem;

import tuan.terrier.Files;
import tuan.terrier.HadoopDistributedFileSystem;

public class TestAnnotTS14 extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(TestAnnotTS14.class);
	
	private static final String DISAMB_OPT = "ned";
	private static final String DISAMB_HDFS_OPT = "dexter.disambiguator";

	private static final String DEXTER_CONF_OPT = "dexter";
	private static final String DEXTER_CONF_PATH_HDFS = "dexter-conf.path";

	// Each JVM has only one dexterParam instance
	private DexterParams dexterParams;
	private Spotter s;
	private Disambiguator d;
	private Tagger tagger;
	private boolean addWikinames;
	private Integer entitiesToAnnotate;
	private double minConfidence;

	private String ned;

	public static final String INPUT_OPT = "in";
	public static final String OUTPUT_OPT = "out";
	public static final String REDUCE_NO = "reduce";
	public static final String JOB_NAME = "name";
	public static final String REMOVE_OUTPUT = "rmo";
	public static final String COMPRESS_OPT = "compress";

	protected CommandLine command;	

	private int reduceNo = 24;
	private String jobName;
	private String input;
	private String output;
	private String compressType = null;	
	private boolean removeOutputDirectory = false;

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

		// load extra options into configuration object
		String dexterConf = null;
		if (!command.hasOption(DEXTER_CONF_OPT)) {
			LOG.error("Dexter configuration path missing");
			return -1;
		}
		dexterConf = command.getOptionValue(DEXTER_CONF_OPT);
		getConf().set(DEXTER_CONF_PATH_HDFS, dexterConf);

		String disambiguator = "tagme";
		if (command.hasOption(DISAMB_OPT)) {
			disambiguator = command.getOptionValue(DISAMB_OPT);
		}
		getConf().set(DISAMB_HDFS_OPT, disambiguator);

		input = command.getOptionValue(INPUT_OPT);
		output = command.getOptionValue(OUTPUT_OPT);

		return 0;
	}

	public void markOutputForDeletion() {
		removeOutputDirectory = true;
	}

	/**
	 * Compress type: gz, bz2, lz4, snappy, lzo
	 * @param type
	 */
	public void setCompress(String type) {
		compressType = type;
	}

	@Override
	public int run(String[] args) throws Exception {

		registerHDFS();
		Configuration conf = getConf();
		
		int prep = parseOtions(args);
		if (prep < 0)
			return prep;

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
		minConfidence = Double.parseDouble("0.2");

		if (ned == null) {
			ned = conf.get(DISAMB_HDFS_OPT);
		}

		InputStream inp = Files.openFileStream(input);
		TTransport transport = new TIOStreamTransport(new XZCompressorInputStream(
				new BufferedInputStream(inp)));
		
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		transport.open();
		
		final StreamItem item = new StreamItem();
		
		while (true) {
			try {
				item.read(protocol);
				System.out.println("docID = " + item.getDoc_id() + ", streamID = " + item.getStream_id() + ", " + item.getStream_time().toString());
				
				if (item.getBody() == null || item.getBody().getClean_visible() == null ||
						item.getBody().getClean_visible().isEmpty()) {
					continue;
				}
				
				String text = item.getBody().getClean_visible();
				MultifieldDocument doc = AnnotTS14.parseDocument(text, "text");		

				EntityMatchList eml = tagger.tag(new DexterLocalParams(), doc);

				AnnotatedDocument adoc = new AnnotatedDocument(doc);

				
				AnnotTS14.annotate(adoc, eml, entitiesToAnnotate, addWikinames, minConfidence);
				
				for (AnnotatedSpot spot : adoc.getSpots()) {
					System.out.println("Mention: " + spot.getMention() 
					 + ", wikipedia page ID: " + spot.getEntity() + ", score: " + spot.getScore());
				}
									
			} catch (TTransportException e) {
				int type = e.getType();
				if (type == TTransportException.END_OF_FILE) {
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
				return -1;
			}
		}
		transport.close();
		
		return 0;
	}

	/** Register the HDFS filesystem to tuan.terrier.Files, so that any libraries use the class
	 * to open files can both read in local and HDFS file systems 
	 * @throws IOException */
	public void registerHDFS() throws IOException {
		if (!Files.hasFileSystemScheme(HadoopDistributedFileSystem.HDFS_SCHEME)) {
			Files.addFileSystemCapability(new HadoopDistributedFileSystem(getConf()));	
		}
	}

	// TODO: Change this by a test case with specific arguments
	public static void main(String[] args) {
		try {
			ToolRunner.run(new TestAnnotTS14(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
