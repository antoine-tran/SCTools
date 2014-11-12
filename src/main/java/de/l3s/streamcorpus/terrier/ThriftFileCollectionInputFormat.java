package de.l3s.streamcorpus.terrier;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import ilps.hadoop.StreamItemWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * An extended NLineInputFormat that reads a list of file containing
 * paths to .sc files (comrpessed or not). It splits the input into
 * different lines, then fetch the thrift records from each files,
 * line by line.
 *
 * @author tuan
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ThriftFileCollectionInputFormat extends FileInputFormat<Text, 
		StreamItemWritable> { 

	public static final String LINES_PER_MAP = 
			"mapreduce.input.lineinputformat.linespermap";

	public RecordReader<Text, StreamItemWritable> createRecordReader(
			InputSplit split, TaskAttemptContext tac) throws IOException {		
		return new ThriftFileCollectionRecordReader();
	}

	/** 
	 * Logically splits the set of input files for the job, splits N lines
	 * of the input as one split.
	 * 
	 * @see FileInputFormat#getSplits(JobContext)
	 */
	public List<InputSplit> getSplits(JobContext job)
			throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int numLinesPerSplit = getNumLinesPerSplit(job);
		for (FileStatus status : listStatus(job)) {
			splits.addAll(getSplitsForFile(status,
					job.getConfiguration(), numLinesPerSplit));
		}
		return splits;
	}

	public static List<FileSplit> getSplitsForFile(FileStatus status,
			Configuration conf, int numLinesPerSplit) throws IOException {
		List<FileSplit> splits = new ArrayList<FileSplit> ();
		Path fileName = status.getPath();
		if (status.isDirectory()) {
			throw new IOException("Not a file: " + fileName);
		}
		FileSystem  fs = fileName.getFileSystem(conf);
		LineReader lr = null;
		try {
			FSDataInputStream in  = fs.open(fileName);
			lr = new LineReader(in, conf);
			Text line = new Text();
			int numLines = 0;
			long begin = 0;
			long length = 0;
			int num = -1;
			while ((num = lr.readLine(line)) > 0) {
				numLines++;
				length += num;
				if (numLines == numLinesPerSplit) {
					splits.add(createFileSplit(fileName, begin, length));
					begin += length;
					length = 0;
					numLines = 0;
				}
			}
			if (numLines != 0) {
				splits.add(createFileSplit(fileName, begin, length));
			}
		} finally {
			if (lr != null) {
				lr.close();
			}
		}
		return splits; 
	}

	/**
	 * NLineInputFormat uses LineRecordReader, which always reads
	 * (and consumes) at least one character out of its upper split
	 * boundary. So to make sure that each mapper gets N lines, we
	 * move back the upper split limits of each split 
	 * by one character here.
	 * @param fileName  Path of file
	 * @param begin  the position of the first byte in the file to process
	 * @param length  number of bytes in InputSplit
	 * @return  FileSplit
	 */
	protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
		return (begin == 0) 
				? new FileSplit(fileName, begin, length - 1, new String[] {})
		: new FileSplit(fileName, begin - 1, length, new String[] {});
	}

	/**
	 * Set the number of lines per split
	 * @param job the job to modify
	 * @param numLines the number of lines per split
	 */
	public static void setNumLinesPerSplit(Job job, int numLines) {
		job.getConfiguration().setInt(LINES_PER_MAP, numLines);
	}

	/**
	 * Get the number of lines per split
	 * @param job the job
	 * @return the number of lines per split
	 */
	public static int getNumLinesPerSplit(JobContext job) {
		return job.getConfiguration().getInt(LINES_PER_MAP, 1);
	}
}