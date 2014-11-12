/*******************************************************************************
 * Copyright 2012 Edgar Meij
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.l3s.streamcorpus.terrier;

import ilps.hadoop.StreamItemWritable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terrier.utility.io.CountingInputStream;


/**
 * RecordReader that gets a file split containing urls of streamcorpus files (rather
 * than the files themselves) and emit the filename together with the stream items. 
 * 
 * @author tuan
 */
public class ThriftFileCollectionRecordReader extends RecordReader<Text,
		StreamItemWritable> {

	private static final Logger LOG = LoggerFactory
			.getLogger(ThriftFileCollectionRecordReader.class);

	// References to current input stream
	private FSDataInputStream fis;
	private BufferedInputStream bis;
	private XZCompressorInputStream xzis;
	private CountingInputStream cis;
	private TTransport transport;
	private TProtocol tp;

	private FileSystem fs;

	/** number of files obtained thus far by this record reader */
	protected int collectionIndex;

	private long start;
	private long length;
	private long position;
	private Text key = new Text();
	private StreamItemWritable value = new StreamItemWritable();
	private TBinaryProtocol.Factory factory;
	private Configuration conf;

	/** list of all paths */
	private List<String> paths;

	@Override
	public void close() throws IOException {
		if (transport != null) transport.close();
		if (cis != null) {
			cis.close();
			cis = null;
		}
		if (xzis != null) xzis.close();
		if (bis != null) bis.close();
		if (fis != null) fis.close();
	}

	/** Returns our progress within the split, as a float between 0 and 1. */
	@Override
	public float getProgress() {

		if (paths == null || paths.isEmpty()) {
			return 0.0f;
		}

		if (length == 0) {
			return ((float)collectionIndex)/paths.size();
		}

		float fileProgress = 0;
		if (fis != null && length != start)
			fileProgress = (float)position/(float)(length - start);
		return Math.min(1.0f, (fileProgress + (float)collectionIndex)/paths.size());
	}

	/** 
	 * Read the urls / paths of files from the input file
	 * */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		collectionIndex = -1;

		conf = context.getConfiguration();
		fs = FileSystem.get(conf);

		loadPathsFromInputSplit(split, conf);
	}
	
	
	/** 
	 * Reading a bunch of lines of file paths in a list.
	 * The code in this method is redistributed from Hadoop LineRecordReader
	 * 
	 * @throws IOException 
	 */
	private void loadPathsFromInputSplit(InputSplit split, Configuration conf) 
			throws IOException {
		FileSplit fileSplit = (FileSplit) split;
		Path path = fileSplit.getPath();

		long begin = fileSplit.getStart();
		long end = begin + fileSplit.getLength();

		LOG.info("Reading paths in file " + path.getName());

		// First check the compression codec
		CompressionCodecFactory compressionCodec 
		= new CompressionCodecFactory(conf);
		CompressionCodec codec = compressionCodec.getCodec(path);
		FSDataInputStream fis = fs.open(path);
		SplitLineReader in;

		Seekable filePosition;

		boolean compressed = false;
		Decompressor decompressor = null;
		if (null!=codec) {
			compressed = true;
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn =
						((SplittableCompressionCodec)codec).createInputStream(
								fis, decompressor, begin, end,
								SplittableCompressionCodec.READ_MODE.BYBLOCK);
				in = new CompressedSplitLineReader(cIn, conf, (byte[])null);
				begin = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn;
			} else {
				in = new SplitLineReader(codec.createInputStream(fis,
						decompressor), conf, null);
				filePosition = fis;
			}
		} else {
			fis.seek(begin);
			in = new SplitLineReader(fis, conf, (byte[])null);
			filePosition = fis;
		}
		// If this is not the first split, we always throw away first record
		// because we always (except the last split) read one extra line in
		// next() method.
		if (begin != 0) {
			begin += in.readLine(new Text(), 0,
					maxBytesToConsume(compressed, begin, end));
		}
		long pos = begin;

		int newSize = 0;
		final Text nextLine = new Text();
		paths = new ArrayList<>();
		while (getFilePosition(compressed, filePosition, pos) <= end 
				|| in.needAdditionalRecordAfterSplit()) {

			if (pos == 0) {
				// Strip BOM(Byte Order Mark)
				// Text only support UTF-8, we only need to check UTF-8 BOM
				// (0xEF,0xBB,0xBF) at the start of the text stream.
				newSize = in.readLine(nextLine, Integer.MAX_VALUE, 
						Integer.MAX_VALUE);				
				pos += newSize;
				int textLength = nextLine.getLength();
				byte[] textBytes = nextLine.getBytes();
				if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
						(textBytes[1] == (byte)0xBB) && 
						(textBytes[2] == (byte)0xBF)) {
					// find UTF-8 BOM, strip it.
					LOG.info("Found UTF-8 BOM and skipped it");
					textLength -= 3;
					newSize -= 3;
					if (textLength > 0) {
						// It may work to use the same buffer and 
						// not do the copyBytes
						textBytes = nextLine.copyBytes();
						nextLine.set(textBytes, 3, textLength);
					} else {
						nextLine.clear();
					}
				}
			}
			else {
				newSize = in.readLine(nextLine, Integer.MAX_VALUE, 
						maxBytesToConsume(compressed, pos, end));
				pos += newSize;
			}

			paths.add(nextLine.toString());
			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos " + 
					(pos - newSize));
		}

		try {
			if (in != null) {
				in.close();
			}
			if (fis != null) {
				fis.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
		}
	}
	

	private static int maxBytesToConsume(boolean isCompressedInput,
			long pos, long end) {
		return isCompressedInput ? Integer.MAX_VALUE
				: (int) Math.max(Math.min(Integer.MAX_VALUE, end-pos),
						Integer.MAX_VALUE);
	}

	private static long getFilePosition(boolean isCompressedInput, 
			Seekable filePosition, long pos) throws IOException {
		long retVal;
		if (isCompressedInput && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	/** Open the next file in the collections 
	 * @throws IOException */
	private boolean getNextFile() throws IOException {

		if (paths == null || paths.isEmpty()) {
			return false;
		}

		// close the current file
		close();

		// no more paths to process
		if (++collectionIndex >= paths.size()) {
			return false;
		}

		Path path = new Path(paths.get(collectionIndex));
		// Some files are corrupted, report them and move on
		try {
			fis = fs.open(path);
			bis = new BufferedInputStream(fis);

			if (paths.get(collectionIndex).endsWith(".xz")) {
				xzis = new XZCompressorInputStream(bis);
				cis = new CountingInputStream(xzis);
			} else {
				xzis = null;
				cis = new CountingInputStream(bis);
			}			
			transport = new TIOStreamTransport(cis);			
			position = start = cis.getPos();
			length = fs.getFileStatus(path).getLen();

		} catch (IOException e) {
			LOG.error("Bad file: ", path.toString());
			e.printStackTrace();
		}

		try {
			if (transport != null) transport.open();

			// Skip this file
			else {
				return getNextFile();
			}
		} catch (TTransportException e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		factory = new TBinaryProtocol.Factory();
		tp = factory.getProtocol(transport);
		value = new StreamItemWritable(factory);

		return true;
	}

	@Override
	/**
	 * parse the next key value, update position and return true
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {

		while (true) {

			// The file is corrupted, skip it
			if (cis == null) {
				if (!getNextFile()) {
					return false;
				}
				else continue;
			}

			key.set(paths.get(collectionIndex));

			// assume the underlying file is opened, read and when the EOF
			// is met, move to the next file
			if (cis.available() > 0) {
				try {
					value.read(tp);
					position = cis.getPos();
					return true;
				} catch (TTransportException e) {				
					int type = e.getType();
					if (type == TTransportException.END_OF_FILE) {
						if (!getNextFile()) {
							return false;
						}
						else continue;
					}
				} catch (TException e) {					
					e.printStackTrace();
					throw new IOException(e);
				}			
			} else {
				if (!getNextFile()) {
					return false;
				}
				else continue;
			}
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public StreamItemWritable getCurrentValue() throws IOException,
	InterruptedException {
		return value;
	}
}