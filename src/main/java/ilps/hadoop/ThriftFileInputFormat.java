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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A ThriftInputFormat is simply a non-splitable FileInputFormat.
 * 
 * @author emeij
 *
 */
public class ThriftFileInputFormat extends
    FileInputFormat<Text, StreamItemWritable> {

  // private static final Log LOG =
  // LogFactory.getLog(ThriftFileInputFormat.class);

  @Override
  public RecordReader<Text, StreamItemWritable> createRecordReader(
      InputSplit split, TaskAttemptContext tac) throws IOException,
      InterruptedException {
    return new ThriftRecordReader((FileSplit) split, tac.getConfiguration());
  }

  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
}