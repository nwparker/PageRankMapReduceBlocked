package p2_mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SimpleMapReduce {
    public enum Counter {
    	RESIDUALS
    }
    
	private static final int MAX_ITERATIONS = 100;
	private static final double DAMPING_FACTOR = 0.75;
	private static final double CONVERGENCE_THRESHOLD = 0.001;

	public static void main(String[] args) throws Exception {
//		outputPath.getFileSystem(conf).delete(outputPath, true);
//		outputPath.getFileSystem(conf).mkdirs(outputPath);

//		Path outputDir = new Path(args[1]);
//		Path inputPath = new Path(outputDir, "nodes_simple.txt");

		// temp
		Path outputDir = new Path(".");
		Path inputPath = new Path("input.txt");
		Path inputFile = new Path("nodes_simple.txt");

		int numNodes = createInputFile(inputFile, inputPath);
		
		boolean converged = false;
	    int iter = 1;
	    while (!converged) {
	      Path jobOutputPath = new Path(outputDir, String.valueOf(iter));

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

	/*
	 * Create the input file for the mapper from our original nodes.txt
	 */
	public static int createInputFile(Path nodes_file, Path targetFile)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = nodes_file.getFileSystem(conf);
		OutputStream os = fs.create(targetFile);

		List<String> node_lines = IOUtils.readLines(fs.open(nodes_file), "UTF8");
		int num_nodes = node_lines.size();
		float initial_rank = 1.0f / num_nodes;
		
		// parse all nodes into Hadoop writeable format
		for(int i = 0; i < num_nodes; i++) {
			String[] node_vals = node_lines.get(i).split("$");
			if (node_vals.length < 2) {
				System.out.println("Line format was mezzed uppp.");
				continue;
			}
			int id = Integer.parseInt(node_vals[0]);
			
			// parse outgoing edges
			String[] outgoing_vals = node_vals[1].split(" ");
			int[] outgoing = new int[outgoing_vals.length];
			for(int k = 0; k < outgoing.length; k++) {
				outgoing[k] = Integer.parseInt(outgoing_vals[k]);
			}
			
			DataOutputBuffer buffer = new DataOutputBuffer();
			Node node = new Node(id, initial_rank, outgoing, null);
			node.write(buffer);
			buffer.writeTo(os);
		}

		os.close();
		return num_nodes;
	}

	public static double calcPageRank(Path inputPath, Path outputPath,
			int numNodes) throws Exception {
		Configuration conf = new Configuration();
		
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
		
//		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		
		if (!job.waitForCompletion(true)) {
			throw new Exception("Something went wrong with the job");
		}

		long convergence = job.getCounters().findCounter(Counter.RESIDUALS).getValue();

		System.out.println("======================================");
		System.out.println("=  Num nodes:           " + numNodes);
//		System.out.println("=  Summed convergence:  " + total_convergence);
		System.out.println("=  Convergence:         " + convergence);
		System.out.println("======================================");
		
		return convergence;
	}

}