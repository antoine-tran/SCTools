package org.terrier.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.log4j.Logger;
import org.terrier.utility.ArrayUtils;

public class MetaIndexSplit extends FileSplit
{
	private static Logger logger = Logger.getLogger(MetaIndexSplit.class);
	
	int startId;
	int endId;
	
	public MetaIndexSplit(){
		super(null, (long)0, (long)0, new String[0]);
	}
	
	public MetaIndexSplit(Path file, long start, long length, String[] hosts, int _startId, int _endId) {
		super(file, start, length, hosts);
		startId = _startId;
		endId = _endId;
	}			
	
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		startId = in.readInt();
		endId = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(startId);
		out.writeInt(endId);
	}
	
	public String toString()
	{
		StringBuilder rtr = new StringBuilder();
		rtr.append("MetaIndexSplit: BlockSize=").append(this.getLength());
		rtr.append(" startAt=").append(+this.getStart());
		try{
			rtr.append(" hosts=");
			rtr.append(ArrayUtils.join(this.getLocations(), ","));
		}
		catch (IOException ioe ) {
			logger.warn("Problem getting locations", ioe);
		}
		rtr.append(" ids=["+startId+","+endId +"]");
		return rtr.toString();
	}
}
