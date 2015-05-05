package p2_mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
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
import org.apache.hadoop.util.StringUtils;

public class SimpleMapReduce {
    public enum Counter {
    	RESIDUALS
    }
    
	private static final int MAX_ITERATIONS = 100;
	private static final double DAMPING_FACTOR = 0.75;
	private static final double CONVERGENCE_THRESHOLD = 0.001;

	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputDir = args[1];

//		outputPath.getFileSystem(conf).delete(outputPath, true);
//		outputPath.getFileSystem(conf).mkdirs(outputPath);

		Path outputPath = new Path(outputDir);
		Path inputPath = new Path(outputPath, "input.txt");

		int numNodes = createInputFile(new Path(inputFile), inputPath);
		
		boolean converged = false;
	    int iter = 1;
	    while (!converged) {
	      Path jobOutputPath = new Path(outputPath, String.valueOf(iter));

	      System.out.println("======================================");
	      System.out.println("=  Iteration:    " + iter);
	      System.out.println("=  Input path:   " + inputPath);
	      System.out.println("=  Output path:  " + jobOutputPath);
	      System.out.println("======================================");

	      converged = calcPageRank(inputPath, jobOutputPath, numNodes) < CONVERGENCE_THRESHOLD * numNodes;
	      inputPath = jobOutputPath;
	      iter++;
	    }
		System.out.println("Convergence is below " + CONVERGENCE_THRESHOLD + ", we're done");
	}

	public static int createInputFile(Path file, Path targetFile)
			throws IOException {
		Configuration conf = new Configuration();

		FileSystem fs = file.getFileSystem(conf);

		List<String> lines = IOUtils.readLines(fs.open(file), "UTF8");
		
		int numNodes = lines.size();
		double initialPageRank = 1.0 / (double) numNodes;

//		PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
//		writer.println("The first line");
//		writer.println("The second line");
//		writer.close();
		
		OutputStream os = fs.create(targetFile);
		for(String line: lines) {
			// stuff
		}
		
//		LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
//		while (iter.hasNext()) {
//			String line = iter.nextLine();
//
//			String[] parts = StringUtils.split(line);
//			Node node = new Node().setPageRank(initialPageRank)
//					.setAdjacentNodeNames(
//							Arrays.copyOfRange(parts, 1, parts.length));
//			IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
//		}
		os.close();
		return numNodes;
	}

	public static double calcPageRank(Path inputPath, Path outputPath,
			int numNodes) throws Exception {
		Configuration conf = new Configuration();
		
//		conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes);

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
		
//		Job job = Job.getInstance(conf, "PageRankJob");
//		job.setJarByClass(Main.class);
//		job.setMapperClass(Map.class);
//		job.setReducerClass(Reduce.class);
//		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		FileInputFormat.setInputPaths(job, inputPath);
//		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(true)) {
			throw new Exception("Something went wrong with the job");
		}

		long convergence = job.getCounters().findCounter(Counter.RESIDUALS).getValue();
//		double convergence = ((double) total_convergence / Reduce.CONVERGENCE_SCALING_FACTOR) / (double) numNodes;

		System.out.println("======================================");
		System.out.println("=  Num nodes:           " + numNodes);
//		System.out.println("=  Summed convergence:  " + total_convergence);
		System.out.println("=  Convergence:         " + convergence);
		System.out.println("======================================");
		
		return convergence;
	}

}