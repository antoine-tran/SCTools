package de.l3s.streamcorpus;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.util.map.MapKI.Entry;

public class TestNEE extends Configured implements Tool {
	
	private static final Logger log = LoggerFactory.getLogger(TestNEE.class);
	
	public static final String INPUT_OPT = "in";
	private String input;
	
	@SuppressWarnings("static-access")
	public Options options() {
		Options opts = new Options();
		
		Option inputOpt = OptionBuilder.withArgName("input-path").hasArg(true)
				.withDescription("input file / directory path (required)")
				.create(INPUT_OPT);
		opts.addOption(inputOpt);
		
		return opts;
	}
	
	public int parseOtions(String[] args) {
		Options opts = options();
		CommandLineParser parser = new GnuParser();
		try {
			CommandLine command = parser.parse(opts, args);
			if (!command.hasOption(INPUT_OPT)) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(getClass().getName(), opts);
				ToolRunner.printGenericCommandUsage(System.out);
				return -1;
			}

			input = command.getOptionValue(INPUT_OPT);
		} catch (ParseException e) {
			System.err.println("Error parsing command line: " + e.getMessage());
			return -1;
		}
		
		log.info(" - input: " + input);
		
		return 0;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (parseOtions(args) != 0) {
			return -1;
		}
		FileSystem fs = FileSystem.get(getConf());
		Path p = new Path(input);
		try (SequenceFile.Reader reader = new SequenceFile.Reader(getConf(),
				Reader.file(p))) {
			
			Text key = new Text();
			HMapSIW value = new HMapSIW();
			while (reader.next(key, value)) {
				log.info(key.toString() + ":\n\t");
				for (Entry<String> item : value.entrySet()) {
					log.info(item.getKey() + ": " + item.getValue());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new TestNEE(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
