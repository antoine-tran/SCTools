package streamcorpus;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import streamcorpus.StreamItem;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * User: jacek
 * Date: 3/8/13
 * Time: 3:38 PM
 */
public final class ReadThrift {
	
	private static enum INDEXABLE {Title, Body, Lemma,
		Serif_PER, Serif_ORG, Serif_LOC, Serif_NATIONALITY, Serif_TITLE, Serif_MISC,
		Lingpipe_PER, Lingpipe_ORG, Lingpipe_LOC, Lingpipe_NATIONALITY, Lingpipe_TITLE, Lingpipe_MISC}; 
	
	public static void main(String[] args) throws ClassCastException, ClassNotFoundException, IOException {
		
		// ArticleExtractor ae = ArticleExtractor.INSTANCE;
		
		GZIPInputStream serializedClassifier = new GZIPInputStream(new BufferedInputStream(new FileInputStream("/home/tuan/Developer/eclipses/eclipse-workspace-standard/event-converter/src/"
				+ "main/resources/stanford-ner/classifiers/conll.4class.distsim.crf.ser.gz")));
		AbstractSequenceClassifier<CoreLabel> classifier = (AbstractSequenceClassifier<CoreLabel>) CRFClassifier.getClassifier(serializedClassifier);
		
		// System.out.println(INDEXABLE.Lemma.toString());
		try {
			// File transport magically doesn't work
			//            TTransport transport = new TFileTransport("test-data/john-smith-tagged-by-lingpipe-0.sc", true);
			TTransport transport = new TIOStreamTransport(new XZCompressorInputStream(
					new BufferedInputStream(
							new FileInputStream("test-data/news-8-b2b2d66ca26fdac2ff5d321a6b2a54b5-b5a0533946328064f96c1400a4fdff6b-bfab55caaca3820a6faf0391a35fca0b.sc.xz"))));
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			transport.open();
			int counter = 0;
			while (true) {
				final StreamItem item = new StreamItem();
				try {
					item.read(protocol);
					System.out.println("counter = " + ++counter);
					System.out.println("docID = " + item.getDoc_id() + ", streamID = " + item.getStream_id() + ", " + item.getStream_time().toString());
					System.out.println(new String(item.abs_url.array(), "UTF-8"));
					// System.out.print("Keys in other_content:");
					
					/*Set<String> keys = item.getOther_content().keySet();
					for (String k : keys) System.out.print("\t" + k + "-" + item.getOther_content().get(k).getSentencesSize());
					System.out.println("Title: " + item.getOther_content().get("title"));
					System.out.println("Keys in body:");
					
					keys = item.getBody().getSentences().keySet();*/
										
					/*List<List<CoreLabel>> out = classifier.classify(text);
				      for (List<CoreLabel> sentence : out) {
				        for (CoreLabel word : sentence) {
				          System.out.print(word.word() + '/' + word.get(CoreAnnotations.AnswerAnnotation.class) + ' ');
				        }
				        System.out.println();
				      }*/
					
					// System.out.println(item.getBody().clean_html);
					// System.out.println(ae.getText(item.getBody().clean_html));
					
					
					/*for (String k : keys) {
						System.out.print("\t" + k + " " + k.length() + "\t:");
						if (item.getBody().getSentences().get(k).size() > 0)
						for (Sentence s : item.getBody().getSentences().get(k)) {	
							for (Token t : s.tokens) {								
								System.out.print(t.token + '(' + t.sentence_pos + ") ");
							}
							System.out.println(". ");
							break;
						}													
						System.out.println();
					}*/
					
					
					break;
				} catch (TTransportException e) {
					int type = e.getType();
					if (type == TTransportException.END_OF_FILE) {
						break;
					}
				}
			}
			transport.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}