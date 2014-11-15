package de.l3s.streamcorpus;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import streamcorpus.StreamItem;
import tuan.io.FileUtility;
import tuan.terrier.Files;
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

public class LocalAnnotTS14 {

	private static final Logger LOG = LoggerFactory.getLogger(LocalAnnotTS14.class);
	
	// Each JVM has only one dexterParam instance
	private DexterParams dexterParams;
	private Spotter s;
	private Disambiguator d;
	private Tagger tagger;
	private boolean addWikinames;
	private Integer entitiesToAnnotate;
	private double minConfidence;
	
	private Writer writer;

	private int batch;
	private StringBuilder sb;
	
	public LocalAnnotTS14() {

		dexterParams = DexterParams.getInstance();
		s = dexterParams.getSpotter("wiki-dictionary");
		d = dexterParams.getDisambiguator("tagme");

		tagger = new StandardTagger("std", s, d);
		addWikinames = new Boolean("true");

		entitiesToAnnotate = new Integer(50);
		minConfidence = Double.parseDouble("0.2");
	}
	
	public void setup(String output) throws IOException {

		batch = 0;
		writer = new FileWriter(output);
		sb = new StringBuilder();
	}
	
	public void finish() throws IOException {	
		if (writer != null) {
			if (sb.length() > 0) {
				writer.write(sb.toString());
				sb.delete(0, sb.length());
			}
			writer.flush();
			writer.close();
		}
	}
	
	public int ned(String input) throws IOException, TTransportException {
		InputStream inp = null;
		TTransport transport = null;
		
		try { 
			// inp = Files.openFileStream(input);
			inp = new FileInputStream(input);
			transport = new TIOStreamTransport(new XZCompressorInputStream(
					new BufferedInputStream(inp)));
			
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			transport.open();
			
			final StreamItem item = new StreamItem();
			
			while (true) {
				try {
					item.read(protocol);
					if (item.getBody() == null || item.getBody().getClean_visible() == null ||
							item.getBody().getClean_visible().isEmpty()) {
						continue;
					}
					
					String text = item.getBody().getClean_visible();
					MultifieldDocument doc = parseDocument(text, "text");
					
					sb.append("\n");
					sb.append(item.getDoc_id());

					EntityMatchList eml = tagger.tag(new DexterLocalParams(), doc);

					AnnotatedDocument adoc = new AnnotatedDocument(doc);

					annotate(adoc, eml, entitiesToAnnotate, addWikinames, minConfidence);
					
					for (AnnotatedSpot spot : adoc.getSpots()) {
						sb.append("\t");
						sb.append(spot.getEntity());
						sb.append(",");
						sb.append(spot.getScore());
						
						batch++;
						
						if (batch % 1000 == 0) {
							writer.write(sb.toString());
							sb.delete(0, sb.length());
							writer.flush();
						}
					}
															
				} catch (TTransportException e) {
					e.printStackTrace();
					int type = e.getType();
					if (type == TTransportException.END_OF_FILE) {
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
					return -1;
				}
			}
		}
		finally {
			if (transport != null)
				transport.close();
			if (inp != null)
				inp.close();
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

	public static void main(String[] args) {
		LocalAnnotTS14 annot = new LocalAnnotTS14();
		for (String line : FileUtility.readLines(args[0])) {
			try {
				int idx = line.lastIndexOf('/');
				annot.setup(args[1] + "/" + line.substring(idx+1));
				int res = annot.ned(line);
				if (res != 0) {
					System.err.println("ERROR: " + line);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("ERROR: " + line);
				continue;
			} catch (TTransportException e) {
				System.err.println("ERROR: " + line);
				continue;
			} finally {
				try {
					annot.finish();
				} catch (IOException e) {
					e.printStackTrace();

					System.err.println("Cannot close: " + line);
				}
			}
		}
	}
}
