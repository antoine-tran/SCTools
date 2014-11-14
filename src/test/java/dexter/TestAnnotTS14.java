package dexter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

import de.l3s.streamcorpus.mapreduce.SCAnnotation;
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

	private String jobName;
	private String input;

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

		opts.addOption(jnameOpt);
		opts.addOption(inputOpt);
		opts.addOption(outputOpt);
		
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

	
		if (command.hasOption(JOB_NAME)) {
			jobName = command.getOptionValue(JOB_NAME);
			jobName = jobName.replace('-',' ');
		} else {
			jobName = this.getClass().getCanonicalName();
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

		return 0;
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
				System.out.println("docID = " + item.getDoc_id() + ", streamID = "
						+ item.getStream_id() + ", " + item.getStream_time().toString());
				
				if (item.getBody() == null || item.getBody().getClean_visible() == null ||
						item.getBody().getClean_visible().isEmpty()) {
					continue;
				}
				
				String text = item.getBody().getClean_visible();
				MultifieldDocument doc = parseDocument(text, "text");		

				EntityMatchList eml = tagger.tag(new DexterLocalParams(), doc);

				AnnotatedDocument adoc = new AnnotatedDocument(doc);

				
				annotate(adoc, eml, entitiesToAnnotate, addWikinames, minConfidence);
				
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
