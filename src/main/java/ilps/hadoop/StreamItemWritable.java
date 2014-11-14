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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import streamcorpus.StreamItem;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TProtocol;

public class StreamItemWritable extends StreamItem implements Writable {

	private static final long serialVersionUID = 1L;

	TSerializer serializer = null;
	TDeserializer deserializer = null;

	public StreamItemWritable() {
		super();
	}
	
	public StreamItemWritable(Factory factory) {
		this();
		serializer = new TSerializer(factory);
		deserializer = new TDeserializer(factory);
	}


	/**
	 * Deserializes this object.
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		try {

			byte[] bytes = serializer.serialize(this);
			WritableUtils.writeVInt(out, bytes.length);
			out.write(bytes, 0, bytes.length);

		} catch (TException e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

	/**
	 * Serializes this object.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		try {

			int length = WritableUtils.readVInt(in);
			byte[] bytes = new byte[length];
			in.readFully(bytes, 0, length);

			deserializer.deserialize(this, bytes);

		} catch (TException e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

}