package de.l3s.streamcorpus.terrier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.terrier.indexing.Collection;
import org.terrier.indexing.Document;
import org.terrier.utility.ApplicationSetup;
import org.terrier.utility.Files;
import org.terrier.utility.io.CountingInputStream;

import streamcorpus.StreamItem;

/**
 * Models a collections of StreamCorpus .sc files (possibly compressed, either
 * in XZ or other formats) using Terrier Collection model
 * 
 * @author tuan
 *
 */
public class StreamCorpusCollection implements Collection, Iterator<Document> {

	private static final Logger logger = Logger.getLogger(StreamCorpusCollection.class);

	/** properties for the current document */	
	protected Map<String,String> DocProperties = null;

	/** current doc id */
	private String docId;

	/** Cached stream item */
	private StreamItem item;

	/** The index in the FilesToProcess of the currently processed file.*/
	protected int FileNumber = -1;

	/** list of files to be processed */
	private ArrayList<String> files = new ArrayList<>();

	/** Indicates whether the end of the collection has been reached.*/
	protected boolean endOfCollection = false;

	/** The reference to raw input stream to move ahead in the collection */
	protected CountingInputStream br;

	/** The registered protocol to read Thrift data */
	private TTransport transport;
	private TBinaryProtocol tp;

	protected Class<? extends Document> documentClass;

	/** Underlying reading streams */
	private CountingInputStream is;

	/** In local mode, Terrier opens a collection.spec file and cache every paths into memory */
	public StreamCorpusCollection() {
		this(ApplicationSetup.COLLECTION_SPEC);
	}

	public StreamCorpusCollection(String collectionSpecFile) {
		loadDocumentClass();		
		readCollectionSpec(collectionSpecFile);
		//open the first file
		try {
			openNextFile();
		} catch (IOException ioe) {
			logger.error("IOException opening first file of collection "
					+ "- is the collection.spec correct?", ioe);
		}
		item = new StreamItem();
	}

	/** In Hadoop mode, Terrier opens a new collection for each file split in HDFS 
	 * @throws IOException */
	public StreamCorpusCollection(InputStream input) throws IOException {		
		files = new ArrayList<>();		
		/*br = input instanceof CountingInputStream ? (CountingInputStream)input 
				: new CountingInputStream(input);	*/

		// open the first file, assuming the file is XZ-compressed. Must have some ways
		// to do this more flexibly in the future
		br = new CountingInputStream(new XZCompressorInputStream(input));	

		transport = new TIOStreamTransport(br);

		try {
			transport.open();
		} catch (TTransportException e) {
			logger.error("Couldn't open file in the collection: ");
			e.printStackTrace();
			transport = null;
		}

		if (transport != null) {
			tp = new TBinaryProtocol(transport);
		}

		item = new StreamItem();
	}

	/** Loads the class that will supply all documents for this Collection.
	 * Set by property <tt>trec.document.class</tt>
	 */
	protected void loadDocumentClass() {
		try{
			documentClass = Class.forName(ApplicationSetup.getProperty("trec.document.class", StreamItemDocument.class.getName())).asSubclass(Document.class);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	protected void readCollectionSpec(String CollectionSpecFilename)
	{
		//reads the collection specification file
		try {
			BufferedReader br2 = Files.openFileReader(CollectionSpecFilename);
			String filename = null;
			files = new ArrayList<String>();
			while ((filename = br2.readLine()) != null) {
				filename = filename.trim();
				if (!filename.startsWith("#") && !filename.equals(""))
					files.add(filename);
			}
			br2.close();
			// logger.info("TRECCollection read collection specification ("+files.size()+" files)");
		} catch (IOException ioe) {
			logger.error("Input output exception while loading the collection.spec file. "
					+ "("+CollectionSpecFilename+")", ioe);
		}
	}

	/**
	 * Check whether it is the end of the collection
	 * @return boolean
	 */
	public boolean hasNext() {
		return !endOfCollection();
	}

	/**
	 * Return next document
	 * @return next document
	 */
	public Document next() {
		if (!nextDocument()) {
			return null;
		}
		return getDocument();
	}

	/**
	 * This is unsupported by this Collection implementation, and
	 * any calls will throw UnsupportedOperationException
	 * Throws UnsupportedOperationException on all invocations */
	public void remove() {
		throw new UnsupportedOperationException("Iterator.remove() not supported");
	}

	@Override
	public Document getDocument() {

		// after calling nextDocument() and get true,
		// the stream item should not be null
		if (item == null) {
			logger.warn("Moved to the next document but found an " +
					"invalid stream item. ");
			return null;
		}

		// load property
		// Should have at least some properties from TRECCollection for compatibility
		DocProperties = new HashMap<String, String>();
		DocProperties.put("stream-id", item.getStream_id());
		DocProperties.put("docno", docId);
		DocProperties.put("offsetInFile", Long.toString(br.getPos()));
		DocProperties.put("filenumber", Integer.toString(FileNumber));
		DocProperties.put("url", item.getSchost());
		DocProperties.put("type", item.getSource());
		DocProperties.put("time", item.getStream_time().getZulu_timestamp());

		return new StreamItemDocument(item, DocProperties);
	}

	@Override
	public boolean nextDocument() {
		//move the stream to the start of the next document
		//try next file if error / eof encountered. (and set endOfCollection if
		// needed) max.term.length indexer.meta.forward.keylens
		boolean bScanning = true;
		scanning:
			while (bScanning) {

				try {
					// value
					if (tp != null) {
						try {
							item.read(tp);

							// This happens in chunk 2011-12-13: body is null !!
							if (item.getBody() == null) {
								continue scanning;
							}
							
							if (item.getBody().getClean_visible() == null) {
								continue scanning;
							}
							
							// detect if the document has at least one non-empty section
							if (item.getBody().getClean_visible().isEmpty()) {
								continue scanning;
							}
							docId = item.getDoc_id();
							logger.debug("Current doc id: " + docId);
							
						} catch (TTransportException e) {				
							int type = e.getType();
							if (type == TTransportException.END_OF_FILE) {
								logger.debug("Reaching end of file");
								if (openNextFile()) {									
									continue scanning;
								} else {
									endOfCollection = true;
									return false;
								}
							}
						} catch (TException e) {
							logger.warn("Error reading . Skip the rest..");
							e.printStackTrace();
							if (openNextFile()) {
								continue scanning;
							} else {
								endOfCollection = true;
								return false;
							}
						}			

					} else if (openNextFile()) {
						continue scanning;
					} else {
						endOfCollection = true;
						return false;
					}
				} catch (IOException e) {
					logger.warn("Error reading. Skip");
					continue scanning;
				}
				bScanning = false;
			}

		if (item != null) {
			return true;
		}
		else if (endOfCollection) {
			return false;
		}
		else {
			throw new RuntimeException("Error getting next doc in file ");
		}
	}

	@Override
	public boolean endOfCollection() {
		return endOfCollection;
	}

	// For compatibility purpose. This method isn't really useful in Hadoop setting
	protected boolean openNextFile() throws IOException {
		// close the currently open file
		if (br != null && files.size() > 0) {
			close();
		}

		// keep trying files
		boolean tryFile = true;
		boolean rtr = false;

		while (tryFile) {
			if (FileNumber < files.size() - 1) {
				FileNumber++;
				String filename = files.get(FileNumber).toString();
				if (! Files.exists(filename)) {
					logger.warn("Could not open "+filename+" : File Not Found");
				}
				else if (! Files.canRead(filename)) {
					logger.warn("Could not open "+filename+" : Cannot read");
				} else {

					// support XZ compression
					if (filename.endsWith(".xz")) {
						br = new CountingInputStream(
								new XZCompressorInputStream(
										Files.openFileStream(filename)));	
					}
					else {
						br = new CountingInputStream(
								Files.openFileStream(filename));
					}					
					transport = new TIOStreamTransport(br);

					try {
						transport.open();
					} catch (TTransportException e) {
						logger.error("Couldn't open file " + filename);
						e.printStackTrace();
						transport = null;
					}

					if (transport != null) {
						tp = new TBinaryProtocol(transport);
						tryFile = false;
						rtr = true;
					}
				}
			} else {
				//last file of the collection has been read, EOC
				endOfCollection = true;
				rtr = false;
				tryFile = false;
			}
		}
		return rtr;
	}

	@Override
	public void reset() {
		FileNumber = -1; 
		endOfCollection = false;
		try {
			openNextFile();
		} catch (IOException ioe) {
			logger.warn("IOException while resetting collection "
					+ "- ie re-opening first file", ioe);
		}

	}


	@Override
	public void close() throws IOException {
		if (transport != null) transport.close();		
		if (br != null) br.close();
	}
}
