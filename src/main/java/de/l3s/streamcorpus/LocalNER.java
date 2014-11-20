package de.l3s.streamcorpus;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Set;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import streamcorpus.EntityType;
import streamcorpus.Sentence;
import streamcorpus.StreamItem;
import streamcorpus.Token;

public class LocalNER {

	public static void main(String[] args) {
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
					System.out.print("Keys in other_content:");

					Set<String> keys = item.getOther_content().keySet();
					for (String k : keys) System.out.print("\t" + k + "-" + item.getOther_content().get(k).getSentencesSize());
					System.out.println();
					System.out.println("Keys in body:");
					keys = item.getBody().getSentences().keySet();
					for (String k : keys) {
						System.out.println("==============");
						System.out.println(k);
						if (item.getBody().getSentences().get(k).size() > 0)
							for (Sentence s : item.getBody().getSentences().get(k)) {
								
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
											System.out.println("\t\t" + sb.toString() + "\t" + et);
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
					}	

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
