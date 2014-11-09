package de.l3s.annot;

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
import it.cnr.isti.hpc.dexter.label.IdHelper;
import it.cnr.isti.hpc.dexter.label.IdHelperFactory;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedDocument;
import it.cnr.isti.hpc.dexter.rest.domain.AnnotatedSpot;
import it.cnr.isti.hpc.dexter.rest.domain.Tagmeta;
import it.cnr.isti.hpc.dexter.spotter.Spotter;
import it.cnr.isti.hpc.dexter.util.DexterLocalParams;
import it.cnr.isti.hpc.dexter.util.DexterParams;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Test the offline version of Tagme, Dexter's implementation version
 */
public class TestTagmeWikiMiner {

	private DexterParams dexterParams;
	private static Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
	public static final IdHelper helper = IdHelperFactory.getStdIdHelper();
	
	private static final Logger logger = LoggerFactory
			.getLogger(TestTagmeWikiMiner.class);
	
	@Before
	public void prepareParams() {
		String confFile = "dexter-conf.xml";
		dexterParams = DexterParams.getInstance();
	}


	@Test
	public void testAnnot1() {
		
		String text = "Dexter is an American television drama series which debuted on"
				+ " Showtime on October 1, 2006. The series centers on Dexter Morgan"
				+ " (Michael C. Hall), a blood spatter pattern analyst for the fictional"
				+ " Miami Metro Police Department (based on the real life Miami-Dade"
				+ " Police Department) who also leads a secret life as a serial killer."
				+ " Set in Miami, the show's first season was largely based on the novel"
				+ " Darkly Dreaming Dexter, the first of the Dexter series novels by Jeff"
				+ " Lindsay. It was adapted for television by screenwriter James Manos,"
				+ " Jr., who wrote the first episode. Subsequent seasons have evolved"
				+ " independently of Lindsay's works.";
		
		Spotter s = dexterParams.getSpotter("wiki-dictionary");

		
		// Uncomment this when running Tagme
		// Disambiguator d = dexterParams.getDisambiguator("tagme");
		
		// Uncomment this when running Wikipedia Miner
		Disambiguator d = dexterParams.getDisambiguator("wikiminer");
		
		Tagger tagger = new StandardTagger("std", s, d);

		boolean addWikinames = new Boolean("true");

		Integer entitiesToAnnotate = new Integer(50);
		double minConfidence = Double.parseDouble("0.2");
		MultifieldDocument doc = parseDocument(text, "text");		

		EntityMatchList eml = tagger.tag(new DexterLocalParams(), doc);

		AnnotatedDocument adoc = new AnnotatedDocument(doc);

		
		annotate(adoc, eml, entitiesToAnnotate, addWikinames, minConfidence);
		
		for (AnnotatedSpot spot : adoc.getSpots()) {
			System.out.println("Mention: " + spot.getMention() + ", wikipedia page ID: " + spot.getEntity() + ", score: " + spot.getScore());
		}
	}
	
	public void annotate(AnnotatedDocument adoc, EntityMatchList eml,
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
	
	private MultifieldDocument getAnnotatedDocument(AnnotatedDocument adoc,
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
					logger.warn(
							"error annotating text output of bound for range {} - {} ",
							pos, em.getStart());
					logger.warn("text: \n\n {}\n\n", currentText);
				}
				// the spot has been normalized, i want to retrieve the real one
				String realSpot = "none";
				try {
					realSpot = currentText
							.substring(em.getStart(), em.getEnd());
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					logger.warn(
							"error annotating text output of bound for range {} - {} ",
							pos, em.getStart());
					logger.warn("text: \n\n {}\n\n", currentText);
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
					logger.warn(
							"error annotating text output of bound for range {} - end ",
							pos);
					logger.warn("text: \n\n {}\n\n", currentText);
				}

			}
			annotated.addField(new Field(field.getName(), sb.toString()));

		}

		return annotated;
	}
	
	private MultifieldDocument parseDocument(String text, String format) {
		Tagmeta.DocumentFormat df = Tagmeta.getDocumentFormat(format);
		MultifieldDocument doc = null;
		if (df == Tagmeta.DocumentFormat.TEXT) {
			doc = new FlatDocument(text);
		}
		if (df == Tagmeta.DocumentFormat.JSON) {
			doc = gson.fromJson(text, MultifieldDocument.class);

		}
		return doc;

	}
}
