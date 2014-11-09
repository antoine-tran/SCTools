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
import it.cnr.isti.hpc.dexter.label.IdHelper;
import it.cnr.isti.hpc.dexter.label.IdHelperFactory;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedDocument;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;
import it.cnr.isti.hpc.dexter.rest.domain.Tagmeta;
import it.cnr.isti.hpc.dexter.spotter.Spotter;
import it.cnr.isti.hpc.dexter.util.DexterLocalParams;
import it.cnr.isti.hpc.dexter.util.DexterParams;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tuan.hadoop.conf.JobConfig;
import tuan.hadoop.io.IntFloatArrayListWritable;

/**
 * Annotate the StreamCorpus dataset with Dexter
 * @author tuan
 *
 */
public class AnnotTS14 extends JobConfig implements Tool {

	private static Logger LOG = LoggerFactory.getLogger(AnnotTS14.class);
	
	private static final String DISAMB_OPT = "ned";
	private static final String DISAMB_HDFS_OPT = "dexter.disambiguator";
	
	private static final String DEXTER_CONF_OPT = "dexter";
	private static final String DEXTER_CONF_PATH_HDFS = "dexter-conf.path";
	
	private static final IdHelper helper = IdHelperFactory.getStdIdHelper();;

	
	private static final class MyMapper 
			extends Mapper<Text, StreamItemWritable, Text, IntFloatArrayListWritable> {

		private final Text keyOut = new Text();
		private final IntFloatArrayListWritable neds = new IntFloatArrayListWritable();
		
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
		}
		
		@Override
		protected void map(Text key, StreamItemWritable item, Context context)
				throws IOException, InterruptedException {
			String docId = item.getDoc_id();
			keyOut.set(docId);
			neds.clear();
			
			if (item.getBody() == null || item.getBody().getClean_visible() == null ||
					item.getBody().getClean_visible().isEmpty()) {
				return;
			}
			
			String text = item.getBody().getClean_visible();
			MultifieldDocument doc = parseDocument(text, "text");		

			EntityMatchList eml = tagger.tag(new DexterLocalParams(), doc);

			AnnotatedDocument adoc = new AnnotatedDocument(doc);

			
			annotate(adoc, eml, entitiesToAnnotate, addWikinames, minConfidence);
			
			for (AnnotatedSpot spot : adoc.getSpots()) {
				// System.out.println("Mention: " + spot.getMention() 
				// + ", wikipedia page ID: " + spot.getEntity() + ", score: " + spot.getScore());
				neds.add(spot.getEntity(), (float) spot.getScore());
			}
			
			context.write(keyOut, neds);
		}		
	}
	
	// Add extra option about the dexter configuration file path in HDFS,
	// and the disambiguator used
	@SuppressWarnings("static-access")
	@Override
	public Options options() {
		Options opts = super.options();
		
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

	@Override
	public int run(String[] args) throws Exception {
		Job job = setup(ThriftFileInputFormat.class, TextOutputFormat.class,
				Text.class, IntFloatArrayListWritable.class,
				Text.class, IntFloatArrayListWritable.class,
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

	private static MultifieldDocument parseDocument(String text, String format) {
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
			if (addWikiNames) {
				spot.setWikiname(helper.getLabel(em.getId()));
			}

			spots.add(spot);
		}
		MultifieldDocument annotatedDocument = getAnnotatedDocument(adoc,
				emlSub);
		adoc.setAnnotatedDocument(annotatedDocument);
	}
	
	private static MultifieldDocument getAnnotatedDocument(AnnotatedDocument adoc,
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
