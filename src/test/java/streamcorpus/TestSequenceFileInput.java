package streamcorpus;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.map.MapKI.Entry;

public class TestSequenceFileInput {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(new Path("/home/tuan/Downloads/part-r-00012")));
		Text key = new Text();
		HMapSIW val = new HMapSIW();
		while (reader.next(key,val)) {
			System.out.println(key);
			for (Entry<String> item : val.entrySet()) {
				System.out.print(item.getKey() + " - " + item.getValue() + "\t");
			}
			System.out.println();
		}		
	}
}
