package org.terrier.structures;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.terrier.structures.CompressingMetaIndex.InputStream;
import org.terrier.utility.ApplicationSetup;
import org.terrier.utility.Files;
import org.terrier.utility.Wrapper;
import org.terrier.utility.io.HadoopUtility;
import org.terrier.utility.io.WrappedIOException;

/** 
 * A Hadoop input format for a compressing meta index (allows the reading of a meta index
 * as input to a MapReduce job.
 */
public class CompressingMetaIndexInputFormat implements InputFormat<IntWritable, Wrapper<String[]>>
{
	private static Logger logger = Logger.getLogger(CompressingMetaIndexInputFormat.class);
	
	static String STRUCTURE_NAME_JC_KEY = "MetaIndexInputStreamRecordReader.structureName";
	/** 
	 * Set structure
	 * @param jc
	 * @param metaStructureName
	 */
	public static void setStructure(JobConf jc, String metaStructureName)
	{
		jc.set(STRUCTURE_NAME_JC_KEY, metaStructureName);
	}	
	
	static class MetaIndexInputStreamRecordReader implements RecordReader<IntWritable, Wrapper<String[]>>
	{
		final InputStream in;
		final int startID;
		final int endID;
		
		public MetaIndexInputStreamRecordReader(IndexOnDisk index, String structureName, int startingDocID, int endingID)
			throws IOException
		{
			in = new InputStream(index, structureName, startingDocID, endingID);
			startID = startingDocID;
			endID = endingID;
		}
		
		public void close() throws IOException {
			in.close();
		}

		public IntWritable createKey() {
			return new IntWritable();
		}

		public Wrapper<String[]> createValue() {
			return new Wrapper<String[]>();
		}

		public long getPos() throws IOException {
			return 0;
		}

		public float getProgress() throws IOException {
			return (float)(in.getIndex() - startID)/(float)(endID - startID);
		}

		public boolean next(IntWritable docid, Wrapper<String[]> values)
				throws IOException
		{
			if (! in.hasNext())
				return false;
			//these methods MUST have this order
			values.setObject(in.next());
			docid.set(in.getIndex());
			return true;
		}
		
	}
	
	
	/** 
	 * {@inheritDoc} 
	 */
	public RecordReader<IntWritable, Wrapper<String[]>> getRecordReader(
			InputSplit _split, JobConf jc, Reporter reporter)
			throws IOException
	{
		HadoopUtility.loadTerrierJob(jc);
		
		logger.debug("load split from :");
		for (String s :  _split.getLocations()) {
			logger.debug(s);
		}
		
		//load the index
		Index.setIndexLoadingProfileAsRetrieval(false);
		IndexOnDisk index = HadoopUtility.fromHConfiguration(jc);
		if (index == null)
			throw new IOException("Index could not be loaded from JobConf: " + Index.getLastIndexLoadError() );
		
		//determine the structure to work on
		String structureName = jc.get(STRUCTURE_NAME_JC_KEY);
		if (structureName == null)
			throw new IOException("JobConf property "+STRUCTURE_NAME_JC_KEY+" not specified");
		
		//get the split
		MetaIndexSplit s = (MetaIndexSplit)_split;
		return new MetaIndexInputStreamRecordReader(index, structureName, s.startId, s.endId);			
	}
	
	private static String[] getHosts(FileStatus fs, FileSystem f, long start, long len) throws IOException
	{
		BlockLocation[] bs = f.getFileBlockLocations(fs, start, len);
		Set<String> hosts = new HashSet<String>();
		for(BlockLocation b : bs)
		{
			for(String host : b.getHosts())
			{
				hosts.add(host);
			}
		}
		return hosts.toArray(new String[0]);
	}
	/** 
	 * {@inheritDoc} 
	 */
	public InputSplit[] getSplits(JobConf jc, int advisedNumberOfSplits)
			throws IOException
	{
		logger.setLevel(Level.DEBUG);
		HadoopUtility.loadTerrierJob(jc);
		List<InputSplit> splits = new ArrayList<InputSplit>(advisedNumberOfSplits);
		IndexOnDisk index = HadoopUtility.fromHConfiguration(jc);
		String structureName = jc.get(STRUCTURE_NAME_JC_KEY);
		
		// Change this to new structures in yarn
		final String dataFilename = index.getPath() + ApplicationSetup.FILE_SEPARATOR + index.getPrefix() + "." + structureName + ".zdata";
		final String indxFilename = index.getPath() + ApplicationSetup.FILE_SEPARATOR + index.getPrefix() + "." + structureName + ".idx";
		
		
		final DataInputStream idx = new DataInputStream(Files.openFileStream(indxFilename));
		FileSystem fSys = FileSystem.get(jc);
		FileStatus fs = fSys.getFileStatus(new Path(dataFilename));
		
		final int entryCount = index.getIntIndexProperty("index."+structureName+".entries", 0);
		long dataFileBlockSize = fs.getBlockSize();
		if (forcedDataFileBlockSize != -1) dataFileBlockSize = forcedDataFileBlockSize;
		logger.debug("Block size for "+ dataFilename + " is " + dataFileBlockSize);
		//logger.debug("FSStatus("+dataFilename+")="+ fs.toString());
		int startingId = 0;
		int currentId = 0;
		long startingBlockLocation = 0;
		long blockSizeSoFar = 0;
		long lastRead = idx.readLong();
		while(++currentId < entryCount)
		{
			lastRead = idx.readLong();
			blockSizeSoFar = lastRead - startingBlockLocation;
			//logger.debug("Offset for docid "+ currentId + " is " + lastRead + " blockSizeSoFar="+blockSizeSoFar + " blockStartsAt="+startingBlockLocation);
			if (blockSizeSoFar > dataFileBlockSize)
			{
				final String[] hosts = getHosts(fs, fSys, startingBlockLocation, blockSizeSoFar);
				MetaIndexSplit s = new MetaIndexSplit(new Path(dataFilename), startingBlockLocation, blockSizeSoFar, hosts, startingId, currentId);
				splits.add(s);
				logger.debug("Got split: "+ s.toString());
				
				blockSizeSoFar = 0;
				startingBlockLocation = lastRead + 1;
				startingId = currentId +1;
			}
		}
		if (startingId < currentId)
		{
			blockSizeSoFar = lastRead - startingBlockLocation;
			final String[] hosts = getHosts(fs, fSys, startingBlockLocation, blockSizeSoFar);
			
			logger.debug("Create new MetaIndexSplit with start = " + startingId + ", end = " + currentId);
			
			MetaIndexSplit s = new MetaIndexSplit(new Path(dataFilename), startingBlockLocation, blockSizeSoFar, hosts, startingId, currentId-1);
			logger.debug("Got last split: "+ s);
			splits.add(s);
		}
		idx.close();
		logger.debug("Got "+ splits.size() + " splits when splitting meta index");
		return splits.toArray(new InputSplit[0]);
	}
	
	long forcedDataFileBlockSize = -1;
	
	/** Permit the blocksize to be overridden, useful for testing different code paths */
	public void overrideDataFileBlockSize(long blocksize)
	{
		forcedDataFileBlockSize = blocksize;
	}
	/** 
	 * Validates the structure based on the job configuration
	 */
	public void validateInput(JobConf jc) throws IOException {
		if (jc.get(STRUCTURE_NAME_JC_KEY, null) == null)
			throw new WrappedIOException(new IllegalArgumentException("Key " + STRUCTURE_NAME_JC_KEY +" not specified"));
	}
	
}
