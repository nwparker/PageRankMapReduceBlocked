package p2_mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/*
 * IN: node id -> list of rank flows
 * OUT: node id -> pagerank
 */
public class Reduce extends Reducer<Node, FloatWritable, IntWritable, FloatWritable> {
	
	private static final float SCALING = 1000;
	private static final float DAMPING_FACTOR = 0.75f;
	
    public void reduce(Node node, Iterable<FloatWritable> rank_flows, Context context)
    throws IOException, InterruptedException {
    	
    	int num_nodes = context.getConfiguration().getInt("num_nodes", -1); // should never be the default of -1
    	float original_pagerank = node.rank;
        float pagerank = 0;
        
        // sum up flow from other nodes for this node
        for (FloatWritable flow : rank_flows)
        	pagerank += flow.get();
        
        // include the damping factor in new pagerank
        float damping_factor = (1 - DAMPING_FACTOR) / (float) num_nodes;
        pagerank = damping_factor + (DAMPING_FACTOR * pagerank);
        
        // emit node and new page rank
        context.write(new IntWritable(node.Id), new FloatWritable(pagerank));
        
        // update the residuals
        long delta = (long) Math.abs(SCALING*(original_pagerank - pagerank));
        context.getCounter(SimpleMapReduce.Counter.RESIDUALS).increment(delta);
    }
}