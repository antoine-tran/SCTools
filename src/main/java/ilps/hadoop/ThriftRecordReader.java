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
package ilps.hadoop;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RecordReader that emits filename, StreamItemWritable pairs. 
 * 
 * @author emeij
 * @author tuan
 *
 */
public class ThriftRecordReader extends RecordReader<Text, StreamItemWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(ThriftRecordReader.class);
	
	private FSDataInputStream fis;
	private BufferedInputStream bis;
	private XZCompressorInputStream xzis;
	private TTransport transport;
	private TProtocol tp;
	private long start;
	private long length;
	private long position;
	private Text key = new Text();
	private StreamItemWritable value;
	private TBinaryProtocol.Factory factory;
	private FileSplit fileSplit;
	private Configuration conf;

	public ThriftRecordReader(FileSplit fileSplit, Configuration conf)
			throws IOException {
		this.fileSplit = fileSplit;
		this.conf = conf;
	}

	@Override
	public void close() throws IOException {
		if (transport != null) transport.close();
		if (xzis != null) xzis.close();
		if (bis != null) bis.close();
		if (fis != null) fis.close();
	}

	/** Returns our progress within the split, as a float between 0 and 1. */
	@Override
	public float getProgress() {

		if (length == 0)
			return 0.0f;

		return Math.min(1.0f, position / (float) length);

	}

	/** 
	 * Boilerplate initialization code for file input streams. 
	 * 
	 * Tuan - Add the .xz decompressor here
	 * */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		conf = context.getConfiguration();
		fileSplit = (FileSplit) split;
		start = fileSplit.getStart();
		length = fileSplit.getLength();
		position = 0;

		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);

		// Some files are corrupted, report them and move on
		try {
			fis = fs.open(path);
			bis = new BufferedInputStream(fis);
			xzis = new XZCompressorInputStream(bis);
			transport = new TIOStreamTransport(xzis);
		} catch (IOException e) {
			LOG.error("Bad file: ", path.toString());
			e.printStackTrace();
		}

		try {
			if (transport != null) transport.open();
			
			// Skip this file
			else {
				fis = null;
				return;
			}
		} catch (TTransportException e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		factory = new TBinaryProtocol.Factory();
		tp = factory.getProtocol(transport);
		value = new StreamItemWritable(factory);

	}

	@Override
	/**
	 * parse the next key value, update position and return true
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {

		// key
		// key.set(fileSplit.getPath().getName().toString());
		key.set(fileSplit.getPath().getName().toString());

		// The file is corrupted, skip it
		if (fis == null) {
			return false;
		}
		
		// value
		if (fis.available() > 0) { // && position - start < length) {

			try {
				value.read(tp);
				position = length - fis.available() - start;
			} catch (TTransportException e) {				
				int type = e.getType();
				if (type == TTransportException.END_OF_FILE) {
					return false;
				}
			} catch (TException e) {
				e.printStackTrace();
				throw new IOException(e);
			}			

		} else {
			return false;
		}

		return true;

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