package p2_mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class SimpleMapReduce {
	public enum Counter {
		RESIDUALS
	}

	private static final int MAX_ITERATIONS = 100;
	private static final double CONVERGENCE_THRESHOLD = 0.001;
	private static final Log LOG = LogFactory.getLog(SimpleMapReduce.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		// Path outputDir = new Path(args[1]);
		// Path inputPath = new Path(outputDir, "nodes_simple.txt");

		// temp
		Path outputDir = new Path("output");
		Path inputDir = new Path("input");
		Path inputPath = new Path("nodes_simple.txt");
		Path originalInput = inputPath;
		// Path inputFile = new Path("nodes_simple.txt");

		// outputDir.getFileSystem(conf).delete(outputDir, true);
		// outputDir.getFileSystem(conf).mkdirs(outputDir);

		int numNodes = getNumNodes(originalInput);

		boolean converged = false;
		int iter = 0;
		while (!converged) {
			Path jobOutputPath = new Path(outputDir, String.valueOf(iter));
			jobOutputPath.getFileSystem(conf).delete(jobOutputPath, true);

			System.out.println("======================================");
			System.out.println("=  Iteration:    " + iter);
			System.out.println("=  Input path:   " + inputPath);
			System.out.println("=  Output path:  " + jobOutputPath);
			System.out.println("======================================");

			converged = calcPageRank(inputPath, jobOutputPath, numNodes) < CONVERGENCE_THRESHOLD
					* numNodes;
			// inputPath = jobOutputPath;
			iter++;
			inputPath = new Path(inputDir, String.valueOf(iter));
			inputPath.getFileSystem(conf).delete(inputPath, true);
			
			formatInputFile(originalInput, jobOutputPath, inputPath);
		}
		System.out.println("Convergence is below " + CONVERGENCE_THRESHOLD
				+ ", we're done");
	}

	public static int getNumNodes(Path path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = path.getFileSystem(conf);
		List<String> lines = IOUtils.readLines(fs.open(path));
		return lines.size();
	}

	/*
	 * Create the input file for the mapper from our original nodes.txt and
	 * updated pageranks
	 */
	public static int formatInputFile(Path originalInput, Path prevOutput,
			Path newInput) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = prevOutput.getFileSystem(conf);
		OutputStream os = fs.create(newInput);

		// Read files
		List<String> originalNodes = IOUtils.readLines(fs.open(originalInput),
				"UTF8");
		List<String> prevOutputNodes = IOUtils.readLines(
				fs.open(originalInput), "UTF8");

		// parse all nodes into Hadoop writeable format
		for (int i = 0; i < originalNodes.size(); i++) {
			if (i % 100000 == 0) {
				System.out.println("writing node " + i);
			}
			String originalLine = originalNodes.get(i).trim();
			String[] prevVals = prevOutputNodes.get(i).split("\\$");

			if (prevVals.length < 2) {
				System.out.println("Line format was mezzed uppp.");
				continue;
			}

			// Add the ranks to the original line
			String newLine = originalLine + "$" + prevVals[1];
			IOUtils.write(newLine, os);
		}

		os.close();
		return originalNodes.size();
	}

	public static double calcPageRank(Path inputPath, Path outputPath,
			int numNodes) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("num_nodes", numNodes);

		Job job = Job.getInstance(conf, "SimpleMapReduce");
		job.setJarByClass(SimpleMapReduce.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		// job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// job.setInputFormatClass(KeyValueTextInputFormat.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Text.class);

		if (!job.waitForCompletion(true)) {
			throw new Exception("Something went wrong with the job");
		}

		org.apache.hadoop.mapreduce.Counter residuals = job.getCounters().findCounter(Counter.RESIDUALS);
		long convergence = residuals.getValue();
		residuals.setValue(0);
		
		System.out.println("======================================");
		System.out.println("=  Num nodes:           " + numNodes);
		// System.out.println("=  Summed convergence:  " + total_convergence);
		System.out.println("=  Convergence:         " + convergence);
		System.out.println("======================================");

		return convergence;
	}

}