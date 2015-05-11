package p2_mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SimpleMapReduce {
	public enum Counter {
		RESIDUALS
	}

	private static final double CONVERGENCE_THRESHOLD = 0.001;
	private static final Log LOG = LogFactory.getLog(SimpleMapReduce.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);

		 Path inputPath = new Path(args[0]);
		 Path outputDir = new Path(args[1]);

		// temp
//		Path outputDir = new Path("output");
//		Path inputPath = new Path("nodes_simple_test1.txt");
		Path originalInput = inputPath;

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
			inputPath = jobOutputPath;
			iter++;
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
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Node.class);

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