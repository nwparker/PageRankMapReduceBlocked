package p2_mapreduce;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * IN: node id -> list of rank flows
 * OUT: node id -> pagerank
 */
public class Reduce extends Reducer<IntWritable, Node, IntWritable, FloatWritable> {
	
	// the precision we want when converting from float to long for the Hadoop counter
	private static final int PRECISION = 1000;
	
    public void reduce(IntWritable block_id, Iterable<Node> nodes, Context context)
    	throws IOException, InterruptedException {
    	
    	// get the page ranks of nodes before updating with page rank algorithm
    	HashMap<Integer, Node> old_node_ranks = new HashMap<Integer, Node>();
    	
    	// get list of nodes to pass into pagerankBoundaries; TODO: change method sig of pagerankBoundaries to take Hashmap?
    	Node[] node_list = new Node[context.getConfiguration().getInt("num_nodes", -1)]; // should never be -1    	
    	Iterator<Node> iter = nodes.iterator();
    	int counter = 0;
    	
    	while(iter.hasNext()) {
    		Node node = (Node) iter.next();
    		old_node_ranks.put(node.Id, node);
    		node_list[counter] = node;
    		counter++;
    	}
    	
    	// calculate new page ranks for nodes in this block
    	HashMap<Integer, Float> new_node_ranks = PageRank.pagerankBoundaries(node_list);
    	
    	// for each node in the block, emit the node id and its new page rank and update residual errors counter
    	for(Entry<Integer, Float> entry : new_node_ranks.entrySet()) {
    		int node_id = entry.getKey();
    		float new_rank = entry.getValue();
    		float old_rank = old_node_ranks.get(node_id).rank;
    		    		
    		// emit node-id with new page rank
    	    context.write(new IntWritable(node_id), new FloatWritable(new_rank));
    	    
    		// update the residual counter
    		long residual = (long) (PRECISION * Math.abs((old_rank - new_rank) / old_rank));
        	context.getCounter(BlockedPageRank.Counter.RESIDUALS).increment(residual);
    	}	
    }
}



